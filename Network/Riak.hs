module Network.Riak where

import Network.Riak.KV
import Network.Riak.Op
import Network.Riak.Monad
import Network.Riak.Types

import Control.Monad.Free
import Data.ByteString
import Data.Time

ping :: Riak m ()
ping = liftF pingOp

get :: KV m [(ByteString, Metadata, Maybe UTCTime)]
get = KV $ \bucketType bucket key -> const (liftF (getOp bucketType bucket key))

put :: ByteString -> Metadata -> KV m [(ByteString, Metadata, Maybe UTCTime)]
put value metadata = KV $ \bucketType bucket key vclock -> 
                            liftF (putOp value metadata bucketType bucket key vclock)
