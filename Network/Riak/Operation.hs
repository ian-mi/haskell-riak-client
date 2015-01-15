{-# LANGUAGE RecordWildCards #-}
module Network.Riak.Operation (pingOp, fetchOp, putOp, putOp_) where

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

pingOp :: MonadThrow m => Riak m ()
pingOp = makeRequest PingRequest (getResponse pingResponse)

fetchOp :: MonadThrow m => Bucket -> Key -> Riak m ([(ByteString, Metadata, Maybe UTCTime)], VClock)
fetchOp (Bucket b) (Key k) = makeRequest req (getResponse fetchResponse)
  where req = GetRequest { getBucket = b
                         , getKey = k
                         , r = Nothing
                         , pr = Nothing
                         , basic_quorom = Nothing
                         , notfound_ok = Nothing
                         , if_modified = Nothing
                         , getHead = Nothing
                         , deleted_vclock = Nothing }

putOp :: MonadThrow m => VClock -> BucketType -> Bucket -> Key -> ByteString -> Metadata -> Riak m ([(ByteString, Metadata, Maybe UTCTime)], VClock)
putOp vclock bucketType bucket key value metadata = makeRequest req (getResponse putResponse)
  where req = putRequest (ReturnBody True) vclock bucketType bucket key value metadata

putOp_ :: MonadThrow m => VClock -> BucketType -> Bucket -> Key -> ByteString -> Metadata -> Riak m ()
putOp_ vclock bucketType bucket key value metadata = makeRequest req (void (getResponse putResponse))
  where req = putRequest (ReturnBody False) vclock bucketType bucket key value metadata
