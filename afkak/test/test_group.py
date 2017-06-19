from twisted.trial import unittest
from mock import Mock, patch
from twisted.internet import defer
from twisted.python.failure import Failure
from afkak.group import Coordinator, ConsumerProtocol, ConsumerGroup
from afkak.common import (
    JoinGroupRequestProtocol, JoinGroupResponseMember,
    SyncGroupMemberAssignment,
    RestartError, RestopError,
    RebalanceInProgressError, ConsumerCoordinatorNotAvailableError,
    NotCoordinatorForConsumerError,
    IllegalGenerationError, UnknownMemberIdError,
    InconsistentGroupProtocolError, RequestTimedOutError, UnknownError,
)


class Base(unittest.TestCase):
    def mock_client(self, coordinator_responses=None):
        client = Mock()
        client._get_clock.return_value.seconds.return_value = 0
        client._send_request_to_coordinator.side_effect = coordinator_responses
        return client

    def make_coordinator(self, client):
        return Coordinator(client, "group_id", ["topic1"], protocol_cls=Mock())

    def join_response(self, member_id="m1", leader_id="m1"):
        return defer.succeed(Mock(
            error_code=0, member_id=member_id, generation_id="g1",
            leader_id=leader_id, members=[]))

    def sync_response(self):
        return defer.succeed(Mock(
            error_code=0, member_assignment=[]))


class TestCoordinator(Base):
    def test_send_join_group_request_success(self):
        client = self.mock_client([
            self.join_response()
        ])
        coord = self.make_coordinator(client)
        de = coord.send_join_group_request()
        self.successResultOf(de)
        self.assertEqual(coord.member_id, "m1")
        self.assertEqual(coord.leader_id, "m1")

    def test_send_join_group_request_failure(self):
        client = self.mock_client([
            defer.fail(RebalanceInProgressError()),
        ])
        coord = self.make_coordinator(client)
        de = coord.send_join_group_request()
        self.successResultOf(de)
        self.assertEqual(coord.member_id, "")
        self.assertEqual(coord.leader_id, None)
        self.assertEqual(coord._rejoin_needed, True)
        self.assertIn("rejoin_needed", repr(coord))

    def test_send_sync_group_request_success(self):
        client = self.mock_client([
            self.join_response(), self.sync_response()
        ])
        coord = self.make_coordinator(client)
        de = coord.send_sync_group_request([])
        self.successResultOf(de)

    def test_send_sync_group_request_failure(self):
        client = self.mock_client([
            defer.fail(RebalanceInProgressError()),
        ])
        coord = self.make_coordinator(client)
        de = coord.send_sync_group_request([])
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, True)

    def test_join_no_broker(self):
        """
            fail to retrieve the coordinator broker and retry
        """
        client = self.mock_client([])
        coord = self.make_coordinator(client)
        coord.get_coordinator_broker = Mock(return_value=None)

        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, True)

    def test_join_sync_leader(self):
        """
            Successfully join, assign as leader, and sync
        """
        client = self.mock_client([
            self.join_response(), self.sync_response()
        ])
        coord = self.make_coordinator(client)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, False)
        self.assertEqual(coord.member_id, "m1")
        self.assertEqual(coord.leader_id, "m1")
        self.assertIn("joined", repr(coord))

    def test_join_sync_follower(self):
        """
            Successfully join and sync as follower
        """
        client = self.mock_client([
            self.join_response(leader_id="m2"), self.sync_response()
        ])
        coord = self.make_coordinator(client)
        de = coord.join_and_sync()
        self.successResultOf(de)

    def test_join_error(self):
        """
            Get an error when joining and retry
        """
        client = self.mock_client([
            defer.fail(RebalanceInProgressError()),
            self.join_response(), self.sync_response()
        ])
        coord = self.make_coordinator(client)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, True)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 1)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, False)

    def test_sync_error(self):
        """
            Get an error when syncing and retry
        """
        client = self.mock_client([
            self.join_response(),
            defer.fail(RebalanceInProgressError()),
            self.join_response(), self.sync_response()
        ])
        coord = self.make_coordinator(client)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, True)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 1)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, False)

    def test_join_fatal_error(self):
        """
            Get an unexpected (non-kafka) error when joining and propagate it
        """
        client = self.mock_client([
            defer.fail(AttributeError()),
        ])
        coord = self.make_coordinator(client)
        start_d = coord._start_d = defer.Deferred()
        de = coord.join_and_sync()
        self.failureResultOf(start_d)
        self.successResultOf(de)

    def test_join_not_needed(self):
        """
            call join_and_sync when not needed
        """
        client = self.mock_client([])
        coord = self.make_coordinator(client)
        _join = coord._join_and_sync = Mock()
        coord._rejoin_d = defer.Deferred()
        coord.join_and_sync()
        _join.assert_not_called()
        coord._rejoin_d = None
        coord._rejoin_needed = False
        coord.join_and_sync()
        _join.assert_not_called()

    def test_heartbeat_resync(self):
        """
            run successful heartbeats, get a resync message, and schedule resync
        """
        client = self.mock_client([
            self.join_response(), self.sync_response(),
            defer.succeed(Mock(error_code=0)),
            defer.fail(RebalanceInProgressError()),
            self.join_response(), self.sync_response(),
        ])
        coord = self.make_coordinator(client)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, False)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 1)
        coord._heartbeat()
        self.assertEqual(coord._rejoin_needed, False)
        self.assertFalse(coord._heartbeat_request_d)
        coord._heartbeat()
        self.assertFalse(coord._heartbeat_request_d)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 2)
        self.assertEqual(coord._rejoin_needed, True)
        coord._heartbeat()
        self.assertEqual(coord._rejoin_needed, True)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 2)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, False)

    def test_heartbeat_coordinator_lost(self):
        """
            run a heartbeat that indicates the coordinator has changed
        """
        client = self.mock_client([
            self.join_response(), self.sync_response(),
            defer.fail(NotCoordinatorForConsumerError()),
        ])
        coord = self.make_coordinator(client)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, False)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 1)
        coord._heartbeat()
        self.assertFalse(coord._heartbeat_request_d)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 2)
        self.assertEqual(coord._rejoin_needed, True)
        client.reset_consumer_group_metadata.assert_called_with(coord.group_id)

    def test_heartbeat_in_progress(self):
        """
            get a heartbeat timer tick while a request is already in progress
        """
        client = self.mock_client([
            self.join_response(), self.sync_response(),
            defer.succeed(Mock(error_code=0)),
        ])
        coord = self.make_coordinator(client)
        de = coord.join_and_sync()
        self.successResultOf(de)
        self.assertEqual(coord._rejoin_needed, False)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 1)
        coord._heartbeat_request_d = defer.Deferred()
        coord.send_heartbeat_request = Mock()
        coord._heartbeat()
        coord.send_heartbeat_request.assert_not_called()
        coord._heartbeat_request_d = None
        coord._stopping = True
        coord._heartbeat()
        coord.send_heartbeat_request.assert_not_called()
        coord._stopping = False
        coord._heartbeat()
        coord.send_heartbeat_request.assert_any_call()

    def test_leave(self):
        """
            send a leavegroup message
        """
        client = self.mock_client([
            defer.succeed(Mock(error_code=0)),
        ])
        coord = self.make_coordinator(client)
        coord.coordinator_broker = Mock()
        coord.member_id = "m1"
        coord.generation_id = "g1"
        de = coord.send_leave_group_request()
        self.successResultOf(de)
        self.assertEqual(coord.member_id, "")
        self.assertIsNone(coord.generation_id)

    def test_start_stop(self):
        client = self.mock_client([
            self.join_response(), self.sync_response(),
        ])
        coord = self.make_coordinator(client)

        start_d = coord.start()
        self.assertNoResult(start_d)
        self.assertRaises(RestartError, coord.start)

        heatbeat_req_d = coord._heartbeat_request_d = defer.Deferred()
        rejoin_wait_d = coord._rejoin_wait_dc = defer.Deferred()
        rejoin_d = coord._rejoin_d = defer.Deferred()
        stop_d = coord.stop()
        self.successResultOf(stop_d)
        self.successResultOf(start_d)
        self.assertEqual(coord.member_id, "")
        self.assertIsNone(coord.protocol)

        self.failureResultOf(heatbeat_req_d)
        self.failureResultOf(rejoin_wait_d)
        self.failureResultOf(rejoin_d)

    def test_stop_not_started(self):
        client = self.mock_client([])
        coord = self.make_coordinator(client)
        stop = coord.stop()
        self.failureResultOf(stop, RestopError)

    def test_double_stop(self):
        client = self.mock_client([])
        coord = self.make_coordinator(client)
        coord._start_d = defer.Deferred()
        coord._stopping = True
        double_stop = coord.stop()
        self.failureResultOf(double_stop, RestopError)

    def test_get_coordinator_success(self):
        """
        retrieve a coordinator
        """
        client = self.mock_client([
            defer.succeed(Mock(error_code=0)),
        ])
        coord = self.make_coordinator(client)
        client._get_coordinator_for_group.return_value = defer.succeed(None)
        client._get_brokerclient.return_value = None
        de = coord.get_coordinator_broker()
        self.successResultOf(de)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 1)

        client._get_coordinator_for_group.return_value = defer.succeed(Mock())
        client._get_brokerclient.return_value = coord
        client.load_metadata_for_topics.return_value = defer.succeed(None)
        de = coord.get_coordinator_broker()
        result = self.successResultOf(de)
        self.assertEqual(result, coord)

    def test_get_coordinator_retry(self):
        """
        fail to retrieve a coordinator and retry
        """
        client = self.mock_client([
            defer.succeed(Mock(error_code=0)),
        ])
        coord = self.make_coordinator(client)
        client._get_coordinator_for_group.return_value = defer.fail(
            ConsumerCoordinatorNotAvailableError())
        client._get_brokerclient.return_value = None
        de = coord.get_coordinator_broker()
        self.successResultOf(de)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 1)
        client._get_coordinator_for_group.return_value = defer.fail(
            UnknownError())
        de = coord.get_coordinator_broker()
        self.successResultOf(de)

    def test_get_coordinator_fatal(self):
        """
        fail to retrieve a coordinator in a non-retriable way
        """
        client = self.mock_client([])
        coord = self.make_coordinator(client)
        client._get_coordinator_for_group.return_value = defer.fail(
            AttributeError())
        de = coord.get_coordinator_broker()
        self.failureResultOf(de)
        self.assertEqual(client._get_clock.return_value.callLater.call_count, 0)

    def test_rejoin_after_error(self):
        """
        try out all the rejoin_after_error scenarios
        """
        client = self.mock_client([])
        coord = self.make_coordinator(client)
        coord.on_group_leave = Mock()

        def check(rejoin_needed, exc):
            coord._rejoin_needed = False
            coord._rejoin_wait_dc = None
            callLater = client._get_clock.return_value.callLater
            callLater.reset_mock()
            client.reset_consumer_group_metadata.reset_mock()
            coord.on_group_leave.reset_mock()

            coord.rejoin_after_error(Failure(exc))
            if rejoin_needed:
                self.assertEqual(coord._rejoin_needed, True)
                self.assertEqual(callLater.call_count, 1)
            else:
                self.assertEqual(coord._rejoin_needed, False)
                self.assertEqual(callLater.call_count, 0)
                self.assertEqual(coord._rejoin_wait_dc, None)

        check(True, RebalanceInProgressError())
        check(True, ConsumerCoordinatorNotAvailableError())
        client.reset_consumer_group_metadata.assert_any_call(coord.group_id)
        check(True, IllegalGenerationError())
        coord.on_group_leave.assert_any_call()
        check(True, UnknownMemberIdError())
        coord.on_group_leave.assert_any_call()
        check(True, InconsistentGroupProtocolError())
        check(True, RequestTimedOutError())
        coord.on_group_leave.assert_any_call()
        check(True, UnknownError())

        coord._stopping = True
        check(False, defer.CancelledError())
        coord._stopping = False

        start_d = coord.start()
        start_d.addErrback(lambda f: None)
        check(False, ValueError())
        coord.on_group_leave.assert_any_call()
        self.successResultOf(start_d)


class TestConsumerProtocol(Base):
    def make_protocol(self):
        return ConsumerProtocol(
            coordinator=Mock(
                topics=["topic1"],
                member_id="member1",
                client=Mock()
            )
        )

    def test_join_group_protocols(self):
        protocol = self.make_protocol()
        self.assertEqual(
            protocol.join_group_protocols(),
            [JoinGroupRequestProtocol(
                "consumer",
                '\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x00'
            )]
        )

    def test_generate_assignments(self):
        protocol = self.make_protocol()
        with patch("afkak.kafkacodec.KafkaCodec.decode_join_group_protocol_metadata", return_value=""):
            protocol.partition_assignment_fn = Mock(return_value={
                "m1": {"topic1": [0]},
                "m2": {"topic2": [0, 1]},
            })
            assignments = protocol.generate_assignments(members=[
                JoinGroupResponseMember("", "m1"),
                JoinGroupResponseMember("", "m2"),
            ])

        self.assertEqual(len(assignments), 2)

    def test_update_assignment(self):
        protocol = self.make_protocol()
        decoded = SyncGroupMemberAssignment({"topic1": [0]}, 0, '')
        with patch("afkak.kafkacodec.KafkaCodec.decode_sync_group_member_assignment", return_value=decoded):
            assignments = protocol.update_assignment("")
            self.assertEqual(assignments, decoded.assignments)


class TestConsumerGroup(Base):
    def test_start_stop(self):
        """
            start a consumergroup, join, and start consumers
        """
        client = self.mock_client([])
        processor = Mock()
        group = ConsumerGroup(client, "group_id", "topic1", processor)
        group.start()
        group.on_join_prepare()
        group.on_join_complete({"topic1": [1, 2, 3]})
        self.assertEqual(len(group.consumers["topic1"]), 3)
        self.assertIn('topic1', repr(group))
        group.stop()
        self.assertEqual(len(group.consumers), 0)

    def test_start_leave(self):
        """
            start a consumergroup, join, start consumers, then get kicked out
        """
        client = self.mock_client([])
        processor = Mock()
        group = ConsumerGroup(client, "group_id", "topic1", processor)
        group.start()
        group.on_join_prepare()
        group.on_join_complete({"topic1": [1, 2, 3]})
        self.assertEqual(len(group.consumers["topic1"]), 3)
        group.on_group_leave()
        self.assertEqual(len(group.consumers), 0)

    def test_shutdown_error(self):
        """
            get errors while shutting down consumers
        """
        client = self.mock_client([])
        processor = Mock()
        group = ConsumerGroup(client, "group_id", "topic1", processor)
        group.start()
        with patch('afkak.group.Consumer', side_effect=[Mock(), Mock()]):
            group.on_join_complete({"topic1": [1, 2]})
            consumer = group.consumers["topic1"][0]
            consumer._start_d = defer.Deferred()
            consumer.shutdown.side_effect = KeyError()
            consumer.stop.side_effect = KeyError()
            consumer2 = group.consumers["topic1"][1]
            consumer2.shutdown.return_value = defer.Deferred()

        de = group.shutdown_consumers()
        self.assertNoResult(de)
        self.assertEqual(len(group.consumers), 0)

        consumer2.shutdown.return_value.errback(KeyError())
        consumer2.stop.assert_called_once_with()
        self.successResultOf(de)

    def test_stop_error(self):
        """
            get errors while stopping consumers
        """
        client = self.mock_client([])
        processor = Mock()
        group = ConsumerGroup(client, "group_id", "topic1", processor)
        group.start()
        with patch('afkak.group.Consumer'):
            group.on_join_complete({"topic1": [1]})
            consumer = group.consumers["topic1"][0]
            consumer.stop.side_effect = KeyError()
        group.stop_consumers()

    def test_consumer_error(self):
        """
            get an unexpected stop error from a consumer
        """
        client = self.mock_client([])
        processor = Mock()
        group = ConsumerGroup(client, "group_id", "topic1", processor)
        start_d = group.start()
        self.assertNoResult(start_d)
        with patch('afkak.group.Consumer') as mock_consumer:
            mock_consumer.return_value.start.return_value = d = defer.Deferred()
            group.on_join_complete({"topic1": [1]})
            self.assertEqual(mock_consumer.return_value.start.called, True)
            d.errback(Failure(AssertionError()))
            self.failureResultOf(start_d, AssertionError)
            d.addErrback(lambda result: None)

    def test_consumer_cancel_during_shutdown(self):
        """
            get an unexpected CancelledError on the start() deferred
            while shutting down a consumer becasue our heartbeat timed out

        """
        client = self.mock_client([])
        processor = Mock()
        group = ConsumerGroup(client, "group_id", "topic1", processor)
        start_d = group.start()
        with patch('afkak.group.Consumer') as mock_consumer:
            consumer_instance = mock_consumer.return_value
            consumer_start_d = defer.Deferred()
            consumer_instance.start.return_value = consumer_start_d
            consumer_instance._start_d = consumer_start_d
            group.on_join_complete({"topic1": [1]})
            self.assertEqual(consumer_instance.start.called, True)

            def stop():
                consumer_start_d.errback(defer.CancelledError())

            consumer_instance.stop.side_effect = stop
            group.rejoin_after_error(Failure(RequestTimedOutError()))

            self.assertEqual(consumer_instance.stop.called, True)
            self.successResultOf(consumer_start_d)
            self.assertNoResult(start_d)

    def test_rejoin_consumer(self):
        client = self.mock_client([])
        processor = Mock()
        group = ConsumerGroup(client, "group_id", "topic1", processor)
        start_d = group.start()
        group.on_group_leave = Mock()
        with patch('afkak.group.Consumer') as mock_consumer:
            mock_consumer.return_value.start.return_value = d = defer.Deferred()
            group.on_join_complete({"topic1": [1]})
            self.assertEqual(mock_consumer.return_value.start.called, True)
            d.errback(Failure(IllegalGenerationError()))
            self.assertEqual(group._rejoin_needed, True)
            self.assertNoResult(start_d)
            group.on_group_leave.assert_any_call()
