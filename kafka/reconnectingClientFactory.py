

# Twisted-related imports
from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, DeferredList, maybeDeferred

log = logging.getLogger("kafka")

DEFAULT_KAFKA_TIMEOUT_SECONDS = 30
MAX_RECONNECT_DELAY_SECONDS = 30
DEFAULT_KAFKA_PORT = 9092
CLIENT_ID = "kafka-python"

class KafkaReconnectingClientFactory(ReconnectingClientFactory):

    # What class protocol instances do we produce?
    protocol = KafkaProtocol

    def __init__(self, client_id=CLIENT_ID,
                 timeout=DEFAULT_KAFKA_TIMEOUT_SECONDS,
                 maxDelay=MAX_RECONNECT_DELAY_SECONDS,
                 maxRetries=None):

        # Set our client id
        self.client_id = client_id

        # If the caller set maxRetries, we will retry that many times to
        # reconnect, otherwise we retry forever
        self.maxRetries = maxRetries

        # Set max delay between reconnect attempts
        self.maxDelay = maxDelay

        # Set our kafka timeout (not network related!)
        self.timeout = timeout

        # The protocol object for the current connection
        self.proto = None


    def buildProtocol(self, addr):
        """
        create & return a KafkaProtocol object, saving it away based
        in self.protos
        """
        log.info('buildProtocol(addr=%r)', addr)
        p = self.protocol()
        return self.proto

    def disconnect(self):
        self.stopTrying()

        if not self.connection:
            log.warning('disconnect called but we are not connected')
            return
        self.connection.disconnect()

    @inlineCallbacks
    def onConnectionUp(self):
        log.info('onConnectionUp: self.proto=%r', self.proto)

        # fire the deferred returned when connect() was called
        if not self.dUp.called:
            self.dUp.callback(self)

        # Notify the user if requested. We call all of the callbacks, but don't
        # wait for any deferreds to fire here. Instead we add them to a deferred
        # list which we check for and wait on before calling any onDisconnectCBs
        # This should keep any state-changes done by these callbacks in the
        # proper order. Note however that the ordering of the individual
        # callbacks in each (connect/disconnect) list isn't guaranteed, and they
        # can all be progressing in parallel if they yield or otherwise deal
        # with deferreds
        dList = []
        for cb in self.onConnectCbs:
            dList.append(maybeDeferred(cb, self))
        self.onConnectDList = DeferredList(dList)

        # Add clearing of self.onConnectDList to the deferredList so that once
        # it fires, it is reset to None
        def clearOnConnectDList(_):
            self.onConnectDList = None

        d.addCallback(clearOnConnectDList)


