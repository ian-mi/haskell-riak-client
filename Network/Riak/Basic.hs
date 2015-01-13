{-# LANGUAGE NamedFieldPuns #-}
module Network.Riak.Basic (connect, ping, fetch, put, put_) where

import Network.Riak.Connection
import Network.Riak.Operation
import Network.Riak.Types

import Control.Concurrent.MVar
import Data.ByteString
import Data.Time

runOp :: Operation IO a -> Connection -> IO a
runOp op (Connection {up, down}) = modifyMVar down (op up)

ping :: Connection -> IO ()
ping = runOp pingOp

fetch :: Bucket -> Key -> Connection -> IO ([(ByteString, Metadata, Maybe UTCTime)], VClock)
fetch bucket key = runOp (fetchOp bucket key)

put :: VClock -> BucketType -> Bucket -> Key -> ByteString -> Metadata -> Connection -> IO ([(ByteString, Metadata, Maybe UTCTime)], VClock)
put vclock bucketType bucket key value metadata = runOp (putOp vclock bucketType bucket key value metadata)

put_ :: VClock -> BucketType -> Bucket -> Key -> ByteString -> Metadata -> Connection -> IO ()
put_ vclock bucketType bucket key value metadata = runOp (putOp_ vclock bucketType bucket key value metadata)
