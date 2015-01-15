{-# LANGUAGE NamedFieldPuns #-}
module Network.Riak.Monad where

import Network.Riak.Connection
import Network.Riak.Message

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad
import Data.Conduit

newtype Riak m a = Riak (Sink Message m () -> ResumableSource m Message -> m (ResumableSource m Message, a))

-- can relax context to Functor in GHC 7.10
instance Monad m => Functor (Riak m) where
         fmap f (Riak m) = Riak (fmap (fmap (liftM (fmap f))) m)

instance Monad m => Applicative (Riak m) where
         pure = Riak . const . (return .) . flip (,)
         (<*>) = ap

-- can remove the definition of return in GHC 7.10
instance Monad m => Monad (Riak m) where
         return = pure
         Riak m >>= f = Riak $ \snk src -> do (src', a) <- m snk src
                                              let Riak n = f a
                                              n snk src'

runRiak :: Connection -> Riak IO a -> IO a
runRiak Connection {up, down} (Riak m) = modifyMVar down (m up)
