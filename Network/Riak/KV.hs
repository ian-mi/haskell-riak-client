module Network.Riak.KV where

import Network.Riak.Monad
import Network.Riak.Types

import Control.Applicative
import Control.Monad

newtype KV m a = KV { unKV :: BucketType -> Bucket -> Key -> VClock -> Riak m (VClock, a) }

instance Monad m => Functor (KV m) where
         fmap f = KV . fmap (fmap (fmap (fmap (fmap (fmap f))))) . unKV
         
instance Monad m => Applicative (KV m) where
         pure = KV . const . const . const . fmap pure . flip (,)
         (<*>) = ap

instance Monad m => Monad (KV m) where
         return = pure
         m >>= f = KV $ \bucketType bucket key vclock -> do (vclock', a) <- unKV m bucketType bucket key vclock
                                                            unKV (f a) bucketType bucket key vclock'

runKV :: Monad m => BucketType -> Bucket -> Key -> KV m a -> Riak m a
runKV bucketType bucket key (KV m) = fmap snd (m bucketType bucket key (VClock Nothing))
