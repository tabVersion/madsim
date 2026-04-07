use futures_util::Stream;
use madsim::net::Endpoint;
use serde::Deserialize;
use spin::Mutex;
use tracing::*;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    broker::FetchOptions,
    client::ClientContext,
    config::{FromClientConfig, FromClientConfigAndContext},
    error::{KafkaError, KafkaResult},
    message::{BorrowedMessage, Message, OwnedMessage},
    metadata::Metadata,
    sim_broker::Request,
    topic_partition_list::Elem,
    util::Timeout,
    ClientConfig, Offset, TopicPartitionList,
};

/// Common trait for all consumers.
pub trait Consumer<C = DefaultConsumerContext>
where
    C: ConsumerContext,
{
    /// Retrieve current positions (offsets) for topics and partitions.
    fn position(&self) -> KafkaResult<TopicPartitionList>;
}

/// Consumer-specific context.
pub trait ConsumerContext: ClientContext {}

/// An inert [`ConsumerContext`] that can be used when no customizations are needed.
#[derive(Clone, Debug, Default)]
pub struct DefaultConsumerContext;

impl ClientContext for DefaultConsumerContext {}
impl ConsumerContext for DefaultConsumerContext {}

/// A low-level consumer that requires manual polling.
///
/// This consumer must be periodically polled to make progress on rebalancing,
/// callbacks and to receive messages.
pub struct BaseConsumer<C = DefaultConsumerContext>
where
    C: ConsumerContext,
{
    _context: C,
    config: ConsumerConfig,
    ep: Endpoint,
    addr: SocketAddr,
    state: Mutex<ConsumerState>,
}

#[derive(Debug)]
struct ConsumerState {
    tpl: TopicPartitionList,
    positions: HashMap<(String, i32), Offset>,
    msgs: VecDeque<OwnedMessage>,
}

#[async_trait::async_trait]
impl FromClientConfig for BaseConsumer {
    async fn from_config(config: &ClientConfig) -> KafkaResult<BaseConsumer> {
        BaseConsumer::from_config_and_context(config, DefaultConsumerContext).await
    }
}

/// Creates a new `BaseConsumer` starting from a `ClientConfig`.
#[async_trait::async_trait]
impl<C: ConsumerContext> FromClientConfigAndContext<C> for BaseConsumer<C> {
    async fn from_config_and_context(
        config: &ClientConfig,
        _context: C,
    ) -> KafkaResult<BaseConsumer<C>> {
        let config_json = serde_json::to_string(&config.conf_map)
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;
        let config: ConsumerConfig = serde_json::from_str(&config_json)
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;
        if config.enable_auto_commit {
            warn!("auto commit is not supported yet. consider set 'enable.auto.commit' = false");
        }
        if config.enable_partition_eof {
            warn!("partition eof is not supported yet");
        }
        if config.group_id.is_some() {
            warn!("group id is ignored");
        }
        let addr: SocketAddr = madsim::net::lookup_host(&config.bootstrap_servers)
            .await
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?
            .next()
            .ok_or_else(|| KafkaError::ClientCreation("invalid host or ip".into()))?;
        let p = BaseConsumer {
            _context,
            config,
            ep: Endpoint::bind("0.0.0.0:0")
                .await
                .map_err(|e| KafkaError::ClientCreation(e.to_string()))?,
            addr,
            state: Mutex::new(ConsumerState {
                tpl: TopicPartitionList::new(),
                positions: HashMap::new(),
                msgs: VecDeque::new(),
            }),
        };
        Ok(p)
    }
}

impl<C> BaseConsumer<C>
where
    C: ConsumerContext,
{
    pub fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        let mut tpl = assignment.clone();
        // auto offset reset
        for e in &mut tpl.list {
            self.reset_offset(e);
        }
        let positions = tpl
            .list
            .iter()
            .map(|elem| ((elem.topic.clone(), elem.partition), Offset::Invalid))
            .collect();
        *self.state.lock() = ConsumerState {
            tpl,
            positions,
            msgs: VecDeque::new(),
        };
        Ok(())
    }

    /// Incrementally adds partitions to the current assignment.
    ///
    /// Existing partitions are left untouched, including their offsets.
    /// Buffered messages from existing partitions are preserved.
    /// To adjust offsets for partitions that are already assigned, call
    /// [`BaseConsumer::seek_partitions`] instead.
    pub fn incremental_assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        let mut new_tpl = assignment.clone();

        for e in &mut new_tpl.list {
            self.reset_offset(e);
        }

        let mut state = self.state.lock();
        let mut assigned: HashSet<(String, i32)> = state
            .tpl
            .list
            .iter()
            .map(|elem| (elem.topic.clone(), elem.partition))
            .collect();

        for new_elem in new_tpl.list {
            let key = (new_elem.topic.clone(), new_elem.partition);
            if assigned.insert(key) {
                state.positions.insert(
                    (new_elem.topic.clone(), new_elem.partition),
                    Offset::Invalid,
                );
                state.tpl.list.push(new_elem);
            }
        }

        Ok(())
    }

    /// Incrementally removes partitions from the current assignment.
    ///
    /// Partitions not present in the current assignment are ignored.
    /// Note: Buffered messages are not cleared. Any messages already
    /// fetched from removed partitions may still be returned by subsequent
    /// poll operations until the buffer is exhausted.
    pub fn incremental_unassign(&self, unassignment: &TopicPartitionList) -> KafkaResult<()> {
        let mut state = self.state.lock();

        if state.tpl.list.is_empty() {
            return Ok(());
        }

        let to_remove: HashSet<(&str, i32)> = unassignment
            .list
            .iter()
            .map(|candidate| (candidate.topic.as_str(), candidate.partition))
            .collect();

        state
            .tpl
            .list
            .retain(|elem| !to_remove.contains(&(elem.topic.as_str(), elem.partition)));
        state
            .positions
            .retain(|(topic, partition), _| !to_remove.contains(&(topic.as_str(), *partition)));

        Ok(())
    }

    /// Adjusts offsets for the partitions that are currently assigned.
    ///
    /// This clears the buffered messages and writes the offsets provided in
    /// `topic_partition_list` back to the internal `TopicPartitionList`.
    /// Only partitions that are already assigned may be updated. The
    /// simulation backend currently rejects `Offset::Stored` and
    /// `Offset::OffsetTail`.
    ///
    /// Returns the provided `TopicPartitionList` so the caller can keep using it.
    ///
    /// # Errors
    ///
    /// * [`KafkaError::Seek`] - Returned when a partition is not assigned or the
    ///   offset value is unsupported/invalid.
    pub async fn seek_partitions(
        &self,
        topic_partition_list: TopicPartitionList,
        timeout: impl Into<Timeout>,
    ) -> KafkaResult<TopicPartitionList> {
        // Seek is instantaneous in the simulator, so the timeout is ignored.
        let _ = timeout.into();

        {
            let mut state = self.state.lock();
            let index_by_key: HashMap<(&str, i32), usize> = state
                .tpl
                .list
                .iter()
                .enumerate()
                .map(|(idx, elem)| ((elem.topic.as_str(), elem.partition), idx))
                .collect();
            let mut target_indices = Vec::with_capacity(topic_partition_list.list.len());

            for requested in &topic_partition_list.list {
                match requested.offset {
                    Offset::Invalid => {
                        return Err(KafkaError::Seek(format!(
                            "invalid offset for {}:{}",
                            requested.topic, requested.partition
                        )));
                    }
                    Offset::Stored => {
                        return Err(KafkaError::Seek(format!(
                            "stored offset is not supported for {}:{}",
                            requested.topic, requested.partition
                        )));
                    }
                    Offset::OffsetTail(_) => {
                        return Err(KafkaError::Seek(format!(
                            "offset tail is not supported for {}:{}",
                            requested.topic, requested.partition
                        )));
                    }
                    _ => {}
                }

                let key = (requested.topic.as_str(), requested.partition);
                let idx = *index_by_key.get(&key).ok_or_else(|| {
                    KafkaError::Seek(format!(
                        "partition {}:{} is not currently assigned",
                        requested.topic, requested.partition
                    ))
                })?;
                target_indices.push(idx);
            }

            state.msgs.clear();

            for (requested, idx) in topic_partition_list
                .list
                .iter()
                .zip(target_indices.into_iter())
            {
                state.tpl.list[idx].offset = requested.offset;
                state.positions.insert(
                    (requested.topic.clone(), requested.partition),
                    Offset::Invalid,
                );
            }
        }

        Ok(topic_partition_list)
    }

    /// Retrieve current positions (offsets) for topics and partitions.
    ///
    /// Mirrors `librdkafka`'s `rd_kafka_position` semantics: each partition's
    /// offset is the last consumed message offset + 1, or [`Offset::Invalid`]
    /// if no message has been consumed since assignment or seek.
    pub fn position(&self) -> KafkaResult<TopicPartitionList> {
        let state = self.state.lock();
        let mut tpl = TopicPartitionList::with_capacity(state.tpl.count());

        for elem in &state.tpl.list {
            let offset = state
                .positions
                .get(&(elem.topic.clone(), elem.partition))
                .copied()
                .unwrap_or(Offset::Invalid);
            tpl.add_partition_offset(&elem.topic, elem.partition, offset)?;
        }

        Ok(tpl)
    }

    fn reset_offset(&self, e: &mut Elem) {
        if e.offset == Offset::Invalid {
            match self.config.auto_offset_reset {
                AutoOffsetResetStrategy::Latest => e.offset = Offset::End,
                AutoOffsetResetStrategy::Earliest => e.offset = Offset::Beginning,
                AutoOffsetResetStrategy::None => {}
            }
        }
    }

    /// Returns the low and high watermarks for a specific topic and partition.
    pub async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        _timeout: impl Into<Timeout>, // TODO: timeout
    ) -> KafkaResult<(i64, i64)> {
        let req = Request::FetchWatermarks {
            topic: topic.to_string(),
            partition,
        };
        let (tx, mut rx) = self.ep.connect1(self.addr).await?;
        tx.send(Box::new(req)).await?;
        *rx.recv().await?.downcast().unwrap()
    }

    pub async fn offsets_for_times(
        &self,
        timestamps: TopicPartitionList,
        _timeout: impl Into<Timeout>, // TODO: timeout
    ) -> KafkaResult<TopicPartitionList> {
        let req = Request::OffsetsForTimes { tpl: timestamps };
        let (tx, mut rx) = self.ep.connect1(self.addr).await?;
        tx.send(Box::new(req)).await?;
        *rx.recv().await?.downcast().unwrap()
    }

    pub async fn fetch_metadata(
        &self,
        topic: Option<&str>,
        _timeout: impl Into<Timeout>, // TODO: timeout
    ) -> KafkaResult<Metadata> {
        let req = Request::FetchMetadata {
            topic: topic.map(|s| s.to_string()),
        };
        let (tx, mut rx) = self.ep.connect1(self.addr).await?;
        tx.send(Box::new(req)).await?;
        *rx.recv().await?.downcast().unwrap()
    }
}

impl<C> BaseConsumer<C>
where
    C: ConsumerContext,
{
    /// Polls the consumer for new messages.
    pub async fn poll(
        &self,
        _timeout: impl Into<Timeout>, // TODO: timeout
    ) -> Option<KafkaResult<BorrowedMessage<'_>>> {
        self.poll_internal()
            .await
            .map(|res| res.map(|msg| msg.borrow()))
            .transpose()
    }

    async fn poll_internal(&self) -> KafkaResult<Option<OwnedMessage>> {
        // FIXME: concurrent call
        if self.state.lock().msgs.is_empty() {
            let tpl = self.state.lock().tpl.clone();
            if tpl.count() == 0 {
                return Ok(None);
            }
            let req = Request::Fetch {
                tpl,
                opts: FetchOptions {
                    fetch_max_bytes: self.config.fetch_max_bytes,
                    max_partition_fetch_bytes: self.config.max_partition_fetch_bytes,
                },
            };
            let (tx, mut rx) = self.ep.connect1(self.addr).await?;
            tx.send(Box::new(req)).await?;
            let rsp = *(rx.recv().await?)
                .downcast::<KafkaResult<(Vec<OwnedMessage>, TopicPartitionList)>>()
                .unwrap();
            let (msgs, tpl) = rsp?;
            if !msgs.is_empty() {
                debug!("fetched {} messages", msgs.len());
            }
            let mut state = self.state.lock();
            state.msgs = VecDeque::from(msgs);
            state.tpl = tpl;
        }
        let mut state = self.state.lock();
        let msg = state.msgs.pop_front();
        if let Some(ref msg) = msg {
            state.positions.insert(
                (msg.topic().to_owned(), msg.partition()),
                Offset::Offset(msg.offset() + 1),
            );
        }
        Ok(msg)
    }
}

impl<C> Consumer<C> for BaseConsumer<C>
where
    C: ConsumerContext,
{
    fn position(&self) -> KafkaResult<TopicPartitionList> {
        BaseConsumer::position(self)
    }
}

/// A high-level consumer with a [`Stream`](futures::Stream) interface.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C = DefaultConsumerContext>
where
    C: ConsumerContext,
{
    base: Arc<BaseConsumer<C>>,
}

#[async_trait::async_trait]
impl FromClientConfig for StreamConsumer {
    async fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer> {
        StreamConsumer::from_config_and_context(config, DefaultConsumerContext).await
    }
}

/// Creates a new `StreamConsumer` starting from a `ClientConfig`.
#[async_trait::async_trait]
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    async fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<StreamConsumer<C>> {
        let base = Arc::new(BaseConsumer::from_config_and_context(config, context).await?);
        Ok(Self { base })
    }
}

impl<C> StreamConsumer<C>
where
    C: ConsumerContext,
{
    pub fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.base.assign(assignment)
    }

    /// Incrementally adds partitions to the current assignment.
    ///
    /// Delegates to [`BaseConsumer::incremental_assign`].
    pub fn incremental_assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.base.incremental_assign(assignment)
    }

    /// Incrementally removes partitions from the current assignment.
    ///
    /// Delegates to [`BaseConsumer::incremental_unassign`].
    pub fn incremental_unassign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.base.incremental_unassign(assignment)
    }

    /// Batch seek that mirrors [`BaseConsumer::seek_partitions`].
    pub async fn seek_partitions(
        &self,
        topic_partition_list: TopicPartitionList,
        timeout: impl Into<Timeout>,
    ) -> KafkaResult<TopicPartitionList> {
        self.base
            .seek_partitions(topic_partition_list, timeout)
            .await
    }

    pub async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: impl Into<Timeout>,
    ) -> KafkaResult<(i64, i64)> {
        self.base.fetch_watermarks(topic, partition, timeout).await
    }

    pub async fn offsets_for_times(
        &self,
        timestamps: TopicPartitionList,
        timeout: impl Into<Timeout>,
    ) -> KafkaResult<TopicPartitionList> {
        self.base.offsets_for_times(timestamps, timeout).await
    }

    pub async fn fetch_metadata(
        &self,
        topic: Option<&str>,
        timeout: impl Into<Timeout>,
    ) -> KafkaResult<Metadata> {
        self.base.fetch_metadata(topic, timeout).await
    }

    /// Retrieve current positions (offsets) for topics and partitions.
    pub fn position(&self) -> KafkaResult<TopicPartitionList> {
        self.base.position()
    }
}

impl<C> StreamConsumer<C>
where
    C: ConsumerContext,
{
    /// Constructs a stream that yields messages from this consumer.
    pub fn stream(&self) -> MessageStream<'_, C> {
        MessageStream {
            consumer: self,
            in_flight: None,
        }
    }
}

impl<C> Consumer<C> for StreamConsumer<C>
where
    C: ConsumerContext,
{
    fn position(&self) -> KafkaResult<TopicPartitionList> {
        StreamConsumer::position(self)
    }
}

pub struct MessageStream<'a, C>
where
    C: ConsumerContext,
{
    consumer: &'a StreamConsumer<C>,
    in_flight: Option<Pin<Box<dyn Future<Output = KafkaResult<Option<OwnedMessage>>> + Send + 'a>>>,
}

impl<'a, C> Stream for MessageStream<'a, C>
where
    C: ConsumerContext,
{
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.in_flight.is_none() {
            let consumer = self.consumer;
            self.in_flight = Some(Box::pin(async move {
                loop {
                    if let Some(msg) = consumer.base.poll_internal().await? {
                        return Ok(Some(msg));
                    }
                    madsim::time::sleep(Duration::from_secs(1)).await;
                }
            }));
        }

        let poll = self
            .in_flight
            .as_mut()
            .expect("in-flight future must exist")
            .as_mut()
            .poll(cx);

        match poll {
            Poll::Ready(Ok(Some(msg))) => {
                self.in_flight = None;
                Poll::Ready(Some(Ok(msg.borrow())))
            }
            Poll::Ready(Ok(None)) => {
                self.in_flight = None;
                Poll::Pending
            }
            Poll::Ready(Err(err)) => {
                self.in_flight = None;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(all(test, madsim))]
mod tests {
    use super::*;
    use crate::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        message::Message,
        producer::{BaseProducer, BaseRecord},
        sim_broker::SimBroker,
    };
    use futures_util::StreamExt;
    use madsim::{net::NetSim, runtime::Handle};
    use std::convert::TryFrom;
    use std::{net::SocketAddr, time::Duration};

    const BROKER_ADDR: &str = "10.0.0.1:50051";
    const BROKER_IP: &str = "10.0.0.1";

    async fn setup_cluster(topic: &str, partitions: usize) {
        let handle = Handle::current();
        let broker_addr = BROKER_ADDR.parse::<SocketAddr>().unwrap();
        NetSim::current().add_dns_record("broker", broker_addr.ip());

        handle
            .create_node()
            .name("test-broker")
            .ip(BROKER_IP.parse().unwrap())
            .build()
            .spawn(async move {
                SimBroker::default().serve(broker_addr).await.unwrap();
            });

        madsim::time::sleep(Duration::from_millis(500)).await;

        let topic = topic.to_string();
        handle
            .create_node()
            .name("test-admin")
            .ip("10.0.0.2".parse().unwrap())
            .build()
            .spawn(async move {
                let partitions = i32::try_from(partitions).expect("partition count overflow");
                let admin = ClientConfig::new()
                    .set("bootstrap.servers", "broker:50051")
                    .create::<AdminClient<_>>()
                    .await
                    .expect("failed to create admin");
                admin
                    .create_topics(
                        &[NewTopic::new(
                            &topic,
                            partitions,
                            TopicReplication::Fixed(1),
                        )],
                        &AdminOptions::new(),
                    )
                    .await
                    .expect("failed to create topic");
            })
            .await
            .unwrap();

        madsim::time::sleep(Duration::from_millis(200)).await;
    }

    async fn make_consumer_with_offset_reset(offset_reset: &str) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", "broker:50051")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", offset_reset)
            .create::<BaseConsumer>()
            .await
            .expect("failed to create consumer")
    }

    async fn make_consumer() -> BaseConsumer {
        make_consumer_with_offset_reset("earliest").await
    }

    async fn make_stream_consumer() -> StreamConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", "broker:50051")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create::<StreamConsumer>()
            .await
            .expect("failed to create stream consumer")
    }

    async fn produce_from_ip(values: &[u8], ip: &str) {
        let handle = Handle::current();
        let values = values.to_vec();
        let ip = ip.parse().unwrap();
        handle
            .create_node()
            .name("test-producer")
            .ip(ip)
            .build()
            .spawn(async move {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", "broker:50051")
                    .create::<BaseProducer>()
                    .await
                    .expect("failed to create producer");

                for v in values {
                    let payload = [v];
                    producer
                        .send(BaseRecord::<(), [u8; 1]>::to("topic").payload(&payload))
                        .expect("send failed");
                }
                producer.flush(None).await.expect("flush failed");
            })
            .await
            .unwrap();
    }

    async fn produce(values: &[u8]) {
        produce_from_ip(values, "10.0.1.10").await
    }

    async fn poll_payload(consumer: &BaseConsumer) -> u8 {
        loop {
            if let Some(res) = consumer.poll(None).await {
                let msg = res.expect("message error");
                if let Some(payload) = msg.payload() {
                    return payload[0];
                }
            }
            madsim::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[madsim::test]
    async fn seek_partitions_basic() {
        setup_cluster("topic", 1).await;
        produce(&[1, 2, 3]).await;

        Handle::current()
            .create_node()
            .name("test-consumer-basic")
            .ip("10.0.2.10".parse().unwrap())
            .build()
            .spawn(async move {
                let consumer = make_consumer().await;
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition("topic", 0);
                consumer.assign(&assignment).unwrap();

                assert_eq!(poll_payload(&consumer).await, 1);
                assert!(!consumer.state.lock().msgs.is_empty());

                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset("topic", 0, Offset::Offset(2))
                    .unwrap();
                consumer
                    .seek_partitions(tpl, Duration::from_secs(1))
                    .await
                    .unwrap();

                assert!(consumer.state.lock().msgs.is_empty());
                assert_eq!(poll_payload(&consumer).await, 3);
            })
            .await
            .unwrap();
    }

    #[madsim::test]
    async fn seek_partitions_rejects_invalid_offset() {
        setup_cluster("topic", 1).await;
        let consumer = make_consumer().await;
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition("topic", 0);
        consumer.assign(&assignment).unwrap();

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("topic", 0, Offset::Invalid)
            .unwrap();
        let err = consumer
            .seek_partitions(tpl, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, KafkaError::Seek(_)));
    }

    #[madsim::test]
    async fn seek_partitions_rejects_stored_offset() {
        setup_cluster("topic", 1).await;
        let consumer = make_consumer().await;
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition("topic", 0);
        consumer.assign(&assignment).unwrap();

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("topic", 0, Offset::Stored)
            .unwrap();
        let err = consumer
            .seek_partitions(tpl, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, KafkaError::Seek(_)));
    }

    #[madsim::test]
    async fn seek_partitions_rejects_unassigned_partition() {
        setup_cluster("topic", 2).await;
        let consumer = make_consumer().await;
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition("topic", 0);
        consumer.assign(&assignment).unwrap();

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("topic", 1, Offset::Offset(0))
            .unwrap();
        let err = consumer
            .seek_partitions(tpl, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, KafkaError::Seek(_)));
    }

    #[madsim::test]
    async fn seek_partitions_error_preserves_buffer() {
        setup_cluster("topic", 1).await;
        produce(&[1, 2]).await;

        Handle::current()
            .create_node()
            .name("test-consumer-error-buffer")
            .ip("10.0.2.12".parse().unwrap())
            .build()
            .spawn(async move {
                let consumer = make_consumer().await;
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition("topic", 0);
                consumer.assign(&assignment).unwrap();

                assert_eq!(poll_payload(&consumer).await, 1);
                assert!(!consumer.state.lock().msgs.is_empty());
                let before_offset = {
                    let state = consumer.state.lock();
                    state
                        .tpl
                        .list
                        .iter()
                        .find(|elem| elem.topic == "topic" && elem.partition == 0)
                        .map(|elem| elem.offset)
                        .unwrap()
                };

                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset("topic", 0, Offset::Invalid)
                    .unwrap();
                let err = consumer
                    .seek_partitions(tpl, Duration::from_secs(1))
                    .await
                    .unwrap_err();
                assert!(matches!(err, KafkaError::Seek(_)));
                assert!(!consumer.state.lock().msgs.is_empty());

                let after_offset = {
                    let state = consumer.state.lock();
                    state
                        .tpl
                        .list
                        .iter()
                        .find(|elem| elem.topic == "topic" && elem.partition == 0)
                        .map(|elem| elem.offset)
                        .unwrap()
                };
                assert_eq!(before_offset, after_offset);
            })
            .await
            .unwrap();
    }

    #[madsim::test]
    async fn seek_partitions_multiple_partitions() {
        setup_cluster("topic", 2).await;
        produce(&[1, 2, 3, 4]).await;

        Handle::current()
            .create_node()
            .name("test-consumer-multi")
            .ip("10.0.2.11".parse().unwrap())
            .build()
            .spawn(async move {
                let consumer = make_consumer().await;
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition("topic", 0);
                assignment.add_partition("topic", 1);
                consumer.assign(&assignment).unwrap();

                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset("topic", 0, Offset::Beginning)
                    .unwrap();
                tpl.add_partition_offset("topic", 1, Offset::Offset(0))
                    .unwrap();
                consumer
                    .seek_partitions(tpl, Duration::from_secs(1))
                    .await
                    .unwrap();
            })
            .await
            .unwrap();
    }

    #[madsim::test]
    async fn seek_partitions_partial_failure_keeps_state() {
        setup_cluster("topic", 2).await;
        produce(&[1, 2, 3]).await;

        Handle::current()
            .create_node()
            .name("test-consumer-partial-failure")
            .ip("10.0.2.13".parse().unwrap())
            .build()
            .spawn(async move {
                let consumer = make_consumer().await;
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition("topic", 0);
                consumer.assign(&assignment).unwrap();

                assert_eq!(poll_payload(&consumer).await, 1);
                assert!(!consumer.state.lock().msgs.is_empty());
                let before_offset = {
                    let state = consumer.state.lock();
                    state
                        .tpl
                        .list
                        .iter()
                        .find(|elem| elem.topic == "topic" && elem.partition == 0)
                        .map(|elem| elem.offset)
                        .unwrap()
                };

                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset("topic", 0, Offset::Offset(0))
                    .unwrap();
                tpl.add_partition_offset("topic", 1, Offset::Offset(0))
                    .unwrap();
                let err = consumer
                    .seek_partitions(tpl, Duration::from_secs(1))
                    .await
                    .unwrap_err();
                assert!(matches!(err, KafkaError::Seek(_)));
                assert!(!consumer.state.lock().msgs.is_empty());

                let after_offset = {
                    let state = consumer.state.lock();
                    state
                        .tpl
                        .list
                        .iter()
                        .find(|elem| elem.topic == "topic" && elem.partition == 0)
                        .map(|elem| elem.offset)
                        .unwrap()
                };
                assert_eq!(before_offset, after_offset);
            })
            .await
            .unwrap();
    }

    #[madsim::test]
    async fn position_tracks_consumed_messages_instead_of_prefetched_messages() {
        setup_cluster("topic", 1).await;
        produce(&[1, 2, 3]).await;

        Handle::current()
            .create_node()
            .name("test-consumer-position")
            .ip("10.0.2.14".parse().unwrap())
            .build()
            .spawn(async move {
                let consumer = make_consumer().await;
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition("topic", 0);
                consumer.assign(&assignment).unwrap();

                let initial = consumer.position().unwrap();
                let initial_offset = initial
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(initial_offset, Offset::Invalid);

                assert_eq!(poll_payload(&consumer).await, 1);
                assert!(!consumer.state.lock().msgs.is_empty());

                let after_first = consumer.position().unwrap();
                let first_offset = after_first
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(first_offset, Offset::Offset(1));

                assert_eq!(poll_payload(&consumer).await, 2);
                let after_second = consumer.position().unwrap();
                let second_offset = after_second
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(second_offset, Offset::Offset(2));
            })
            .await
            .unwrap();
    }

    #[madsim::test]
    async fn position_is_reset_by_seek_until_next_message_is_consumed() {
        setup_cluster("topic", 1).await;
        produce(&[1, 2, 3]).await;

        Handle::current()
            .create_node()
            .name("test-consumer-position-seek")
            .ip("10.0.2.15".parse().unwrap())
            .build()
            .spawn(async move {
                let consumer = make_consumer().await;
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition("topic", 0);
                consumer.assign(&assignment).unwrap();

                assert_eq!(poll_payload(&consumer).await, 1);
                let before_seek = consumer.position().unwrap();
                let before_seek_offset = before_seek
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(before_seek_offset, Offset::Offset(1));

                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset("topic", 0, Offset::Offset(2))
                    .unwrap();
                consumer
                    .seek_partitions(tpl, Duration::from_secs(1))
                    .await
                    .unwrap();

                let after_seek = consumer.position().unwrap();
                let after_seek_offset = after_seek
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(after_seek_offset, Offset::Invalid);

                assert_eq!(poll_payload(&consumer).await, 3);
                let after_poll = consumer.position().unwrap();
                let after_poll_offset = after_poll
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(after_poll_offset, Offset::Offset(3));
            })
            .await
            .unwrap();
    }

    #[madsim::test]
    async fn latest_assignment_skips_existing_messages_until_new_data_arrives() {
        setup_cluster("topic", 1).await;
        produce(&[1, 2]).await;

        Handle::current()
            .create_node()
            .name("test-consumer-latest")
            .ip("10.0.2.16".parse().unwrap())
            .build()
            .spawn(async move {
                let consumer = make_consumer_with_offset_reset("latest").await;
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition("topic", 0);
                consumer.assign(&assignment).unwrap();

                assert!(consumer.poll(Duration::from_millis(10)).await.is_none());
                let initial_position = consumer.position().unwrap();
                let initial_offset = initial_position
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(initial_offset, Offset::Invalid);

                produce_from_ip(&[3], "10.0.1.11").await;

                assert_eq!(poll_payload(&consumer).await, 3);
                let after_poll = consumer.position().unwrap();
                let after_poll_offset = after_poll
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(after_poll_offset, Offset::Offset(3));
            })
            .await
            .unwrap();
    }

    #[madsim::test]
    async fn stream_position_only_advances_after_stream_yields() {
        setup_cluster("topic", 1).await;
        produce(&[1, 2]).await;

        Handle::current()
            .create_node()
            .name("test-stream-consumer-position")
            .ip("10.0.2.17".parse().unwrap())
            .build()
            .spawn(async move {
                let consumer = make_stream_consumer().await;
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition("topic", 0);
                consumer.assign(&assignment).unwrap();

                let initial = consumer.position().unwrap();
                let initial_offset = initial
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(initial_offset, Offset::Invalid);

                madsim::time::sleep(Duration::from_millis(1500)).await;

                let before_next = consumer.position().unwrap();
                let before_next_offset = before_next
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(before_next_offset, Offset::Invalid);

                let mut stream = consumer.stream();
                let message = stream
                    .next()
                    .await
                    .expect("stream ended")
                    .expect("message error");
                assert_eq!(message.payload(), Some(&[1][..]));

                let after_next = consumer.position().unwrap();
                let after_next_offset = after_next
                    .elements_for_topic("topic")
                    .into_iter()
                    .find(|elem| elem.partition() == 0)
                    .unwrap()
                    .offset();
                assert_eq!(after_next_offset, Offset::Offset(1));
            })
            .await
            .unwrap();
    }

    #[madsim::test]
    async fn incremental_assign_adds_unique_partitions() {
        setup_cluster("topic", 2).await;
        let consumer = make_consumer().await;

        let mut initial = TopicPartitionList::new();
        initial.add_partition("topic", 0);
        consumer.assign(&initial).unwrap();
        assert_eq!(consumer.state.lock().tpl.count(), 1);

        let mut to_add = TopicPartitionList::new();
        to_add.add_partition("topic", 1);
        consumer.incremental_assign(&to_add).unwrap();

        {
            let state = consumer.state.lock();
            assert_eq!(state.tpl.count(), 2);
            let added = state
                .tpl
                .list
                .iter()
                .find(|elem| elem.topic == "topic" && elem.partition == 1)
                .expect("partition 1 missing");
            assert_eq!(added.offset, Offset::Beginning);
        }

        consumer.incremental_assign(&to_add).unwrap();
        assert_eq!(consumer.state.lock().tpl.count(), 2);
    }

    #[madsim::test]
    async fn incremental_unassign_removes_partitions() {
        setup_cluster("topic", 2).await;
        let consumer = make_consumer().await;

        let mut initial = TopicPartitionList::new();
        initial.add_partition("topic", 0);
        initial.add_partition("topic", 1);
        consumer.assign(&initial).unwrap();
        assert_eq!(consumer.state.lock().tpl.count(), 2);

        let mut to_remove = TopicPartitionList::new();
        to_remove.add_partition("topic", 1);
        consumer.incremental_unassign(&to_remove).unwrap();

        {
            let state = consumer.state.lock();
            assert_eq!(state.tpl.count(), 1);
            assert!(state
                .tpl
                .list
                .iter()
                .all(|elem| !(elem.topic == "topic" && elem.partition == 1)));
        }

        // Removing a non-existent partition should be a no-op.
        consumer.incremental_unassign(&to_remove).unwrap();
        assert_eq!(consumer.state.lock().tpl.count(), 1);
    }
}

/// Consumers configs.
///
/// <https://kafka.apache.org/documentation/#consumerconfigs>
#[derive(Debug, Default, Deserialize)]
struct ConsumerConfig {
    #[serde(rename = "bootstrap.servers")]
    bootstrap_servers: String,

    #[serde(rename = "group.id")]
    group_id: Option<String>,

    /// If true the consumer's offset will be periodically committed in the background.
    #[serde(
        rename = "enable.auto.commit",
        deserialize_with = "super::from_str",
        default = "default_enable_auto_commit"
    )]
    enable_auto_commit: bool,

    /// The maximum amount of data the server should return for a fetch request.
    #[serde(rename = "fetch.max.bytes", default = "default_fetch_max_bytes")]
    fetch_max_bytes: u32,

    /// The maximum amount of data per-partition the server will return.
    #[serde(
        rename = "max.partition.fetch.bytes",
        alias = "fetch.message.max.bytes",
        default = "default_max_partition_fetch_bytes"
    )]
    max_partition_fetch_bytes: u32,

    /// What to do when there is no initial offset in Kafka or if the current offset does not exist
    /// any more on the server (e.g. because that data has been deleted)
    #[serde(rename = "auto.offset.reset", default = "default_auto_offset_reset")]
    auto_offset_reset: AutoOffsetResetStrategy,

    /// Emit `PartitionEOF` event whenever the consumer reaches the end of a partition.
    #[serde(
        rename = "enable.partition.eof",
        deserialize_with = "super::from_str",
        default = "default_enable_partition_eof"
    )]
    enable_partition_eof: bool,
}

#[derive(Debug, Default, Deserialize)]
enum AutoOffsetResetStrategy {
    #[default]
    #[serde(rename = "latest", alias = "largest", alias = "end")]
    Latest,
    #[serde(rename = "earliest", alias = "smallest", alias = "beginning")]
    Earliest,
    #[serde(rename = "error")]
    None,
}

const fn default_enable_auto_commit() -> bool {
    true
}
const fn default_fetch_max_bytes() -> u32 {
    52428800
}
const fn default_max_partition_fetch_bytes() -> u32 {
    1048576
}
fn default_auto_offset_reset() -> AutoOffsetResetStrategy {
    AutoOffsetResetStrategy::Latest
}
const fn default_enable_partition_eof() -> bool {
    false
}
