{-# LANGUAGE GeneralizedNewtypeDeriving, FlexibleInstances, MultiParamTypeClasses, RankNTypes #-}
module Network.Riak.KV where

import Network.Riak.Monad
import Network.Riak.Op
import Network.Riak.Types
import Network.Riak.Request
import Network.Riak.Response

import Control.Applicative
import Control.Monad
import Control.Monad.Free.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader
import Control.Monad.Trans.State.Strict
import Data.ByteString
import Data.Time

newtype KV m a = KV { unKV :: KVT (Riak m) a } deriving (Functor, Applicative, Monad)

newtype KVT m a = KVT { runKVT :: ReaderT KVOpts (StateT VClock m) a } deriving (Functor, Applicative, Monad)

instance MonadTrans KV where
         lift = KV . lift . lift

instance MonadTrans KVT where
         lift = KVT . lift . lift

instance Monad m => MonadFree (KVT Op) (KV m) where
         wrap = join . KV . mapKVT liftF

runKV :: Monad m => BucketType -> ByteString -> ByteString -> KV m a -> Riak m a
runKV bucketType bucket key (KV (KVT m)) = fmap fst (runStateT (runReaderT m env) (VClock Nothing))
  where env = KVOpts bucketType bucket key

makeKVT :: (KVOpts -> VClock -> m (a, VClock)) -> KVT m a
makeKVT = KVT . ReaderT . fmap StateT

mapKVT :: (forall a. m a -> n a) -> KVT m b -> KVT n b
mapKVT f = KVT . mapReaderT (mapStateT f) . runKVT

getOp :: KVT Op ([(ByteString, Metadata, Maybe UTCTime)])
getOp = makeKVT $ \opts -> const (Op (getRequest opts) (liftF getResponse))

putOp :: ByteString -> Metadata -> KVT Op [(ByteString, Metadata, Maybe UTCTime)]
putOp value metadata = makeKVT $ \opts vclock -> Op (putRequest value metadata (ReturnBody True) opts vclock)
                                                    (liftF putResponse)

deleteOp :: KVT Op ()
deleteOp = makeKVT $ \opts vclock -> Op (deleteRequest opts vclock) (liftF deleteResponse)
