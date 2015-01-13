{-# LANGUAGE RecordWildCards #-}
module Network.Riak.Operation (Operation, pingOp, fetchOp, putOp, putOp_) where

import Network.Riak.Message
import Network.Riak.Request
import Network.Riak.Response
import Network.Riak.Types

import Control.Monad
import Control.Monad.Catch
import Data.ByteString
import Data.Conduit
import Data.Time

type Operation m a = Sink Message m () -> ResumableSource m Message -> m (ResumableSource m Message, a)

performSimpleOperation :: Monad m => Message -> Sink Message m a -> Operation m a
performSimpleOperation req get up down = do yield req $$ up
                                            down $$++ get

pingOp :: MonadThrow m => Operation m ()
pingOp = performSimpleOperation PingRequest (getResponse pingResponse)

fetchOp :: MonadThrow m => Bucket -> Key -> Operation m ([(ByteString, Metadata, Maybe UTCTime)], VClock)
fetchOp (Bucket b) (Key k) = performSimpleOperation req (getResponse fetchResponse)
  where req = GetRequest { getBucket = b
                         , getKey = k
                         , r = Nothing
                         , pr = Nothing
                         , basic_quorom = Nothing
                         , notfound_ok = Nothing
                         , if_modified = Nothing
                         , getHead = Nothing
                         , deleted_vclock = Nothing }

putOp :: MonadThrow m => VClock -> BucketType -> Bucket -> Key -> ByteString -> Metadata -> Operation m ([(ByteString, Metadata, Maybe UTCTime)], VClock)
putOp vclock bucketType bucket key value metadata = performSimpleOperation req (getResponse putResponse)
  where req = putRequest (ReturnBody True) vclock bucketType bucket key value metadata

putOp_ :: MonadThrow m => VClock -> BucketType -> Bucket -> Key -> ByteString -> Metadata -> Operation m ()
putOp_ vclock bucketType bucket key value metadata = performSimpleOperation req (void (getResponse putResponse))
  where req = putRequest (ReturnBody False) vclock bucketType bucket key value metadata
