{-# LANGUAGE GeneralizedNewtypeDeriving, NamedFieldPuns #-}
module Network.Riak.Monad where

import Network.Riak.Connection
import Network.Riak.Message
import Network.Riak.Op

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Trans.Class
import Control.Monad.Trans.Free.Church
import Data.Conduit

newtype Riak m a = Riak (FT Op m a) deriving (Functor, Applicative, Monad, MonadTrans, MonadFree Op)

riakConduit :: Monad m => Riak m a -> ConduitM Message Message m a
riakConduit (Riak m) = iterTM join (transFT opConduit m)

runRiak :: Connection -> Riak IO a -> IO a
runRiak Connection {up, down} m = modifyMVar down ($$++ fuseUpstream (riakConduit m) up)
