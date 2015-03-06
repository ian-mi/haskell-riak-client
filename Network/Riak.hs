module Network.Riak where

import Network.Riak.KV
import Network.Riak.Op
import Network.Riak.Monad
import Network.Riak.Types

import Control.Monad.Free
import Data.ByteString
import Data.Time

ping :: Monad m => Riak m ()
ping = liftF pingOp

get :: Monad m => KV m [(ByteString, Metadata, Maybe UTCTime)]
get = liftF getOp

put :: Monad m => ByteString -> Metadata -> KV m [(ByteString, Metadata, Maybe UTCTime)]
put value metadata = liftF (putOp value metadata)

delete :: Monad m => KV m ()
delete = liftF deleteOp
