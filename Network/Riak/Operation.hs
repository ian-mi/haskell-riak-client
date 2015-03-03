{-# LANGUAGE RecordWildCards #-}
module Network.Riak.Operation (ping, fetch, put, put_) where

import Network.Riak.KV
import Network.Riak.Message
import Network.Riak.Monad
import Network.Riak.Request
import Network.Riak.Response
import Network.Riak.Types

import Control.Monad
import Control.Monad.Catch
import Data.ByteString
import Data.Conduit
import Data.Time

makeRequest :: Monad m => Message -> Sink Message m a -> Riak m a
makeRequest req getRsp = Riak $ \up down -> do yield req $$ up
                                               down $$++ getRsp

ping :: MonadThrow m => Riak m ()
ping = makeRequest PingRequest (getResponse pingResponse)

fetch :: MonadThrow m => KV m [(ByteString, Metadata, Maybe UTCTime)]
fetch = KV $ \bucketType bucket key _ -> makeRequest (getRequest bucketType bucket key) (getResponse fetchResponse)

put :: MonadThrow m => ByteString -> Metadata -> KV m [(ByteString, Metadata, Maybe UTCTime)]
put value metadata = KV $ \bucketType bucket key vclock -> 
                            makeRequest (putRequest (ReturnBody True) vclock bucketType bucket key value metadata)
                                        (getResponse putResponse)

put_ :: MonadThrow m => ByteString -> Metadata -> KV m ()
put_ value metadata = void $ KV $ \bucketType bucket key vclock -> 
                             makeRequest (putRequest (ReturnBody False) vclock bucketType bucket key value metadata) 
                                         (getResponse putResponse)
