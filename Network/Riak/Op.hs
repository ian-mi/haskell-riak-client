{-# LANGUAGE DeriveFunctor, RankNTypes #-}
module Network.Riak.Op where

import Network.Riak.Message
import Network.Riak.Request
import Network.Riak.Response
import Network.Riak.Types

import Control.Exception
import Control.Monad
import Control.Monad.Free.Church
import Data.ByteString
import Data.Conduit
import Data.Maybe
import Data.Time

data Op a = Op Message (F ((->) Message) a) deriving Functor

pingOp :: Op ()
pingOp = Op PingRequest (liftF pingResponse)

getOp :: BucketType -> Bucket -> Key -> Op (VClock, [(ByteString, Metadata, Maybe UTCTime)])
getOp bucketType bucket key = Op (getRequest bucketType bucket key) (liftF getResponse)

putOp :: ByteString -> Metadata -> BucketType -> Bucket -> Key -> VClock -> Op (VClock, [(ByteString, Metadata, Maybe UTCTime)])
putOp value metadata bucketType bucket key vclock =
  Op (putRequest (ReturnBody True) vclock bucketType bucket key value metadata) (liftF putResponse)

opConduit :: Monad m => Op a -> ConduitM Message Message m a
opConduit (Op req rsp) = yield req >> retract (hoistF (awaitThrow ConnectionClosed) rsp)

awaitThrow :: (Monad m, Exception e) => e -> (a -> b) -> Consumer a m b
awaitThrow e f = liftM (f . fromMaybe (throw e)) await
