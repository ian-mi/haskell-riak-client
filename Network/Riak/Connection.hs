{-# LANGUAGE NamedFieldPuns #-}
module Network.Riak.Connection where

import Network.Riak.Message

import Control.Concurrent.MVar
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Conduit.Network
import Data.Serialize
import Network.Socket as Net
import Network.Socket.ByteString.Lazy

data Connection = Connection {
    down :: MVar (ResumableSource IO Message),
    up :: Sink Message IO () }

newConnection :: Socket -> IO Connection
newConnection s = do srcVar <- newMVar src
                     return (Connection srcVar snk)
  where src = newResumableSource (sourceSocket s) $=+ parseMessages
        snk = CL.mapM_ (sendAll s . runPutLazy . putMessage)

connect :: HostName -> ServiceName -> IO Connection
connect host port = getAddrInfo (Just hints) (Just host) (Just port) >>= connectSock . Prelude.head >>= newConnection
  where hints = defaultHints {addrFlags = [AI_ADDRCONFIG], addrSocketType = Stream}

connectSock :: AddrInfo -> IO Socket
connectSock (AddrInfo {addrFamily, addrSocketType, addrProtocol, addrAddress}) = 
  do sock <- socket addrFamily addrSocketType addrProtocol
     Net.connect sock addrAddress
     return sock
