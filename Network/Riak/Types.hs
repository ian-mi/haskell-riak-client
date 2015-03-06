module Network.Riak.Types where

import Data.ByteString
import Data.Time.Clock

data BucketType = Default | BucketType ByteString

newtype VClock = VClock {vclock :: Maybe ByteString}

data Metadata = Metadata { contentType :: Maybe ByteString
                         , charset :: Maybe ByteString
                         , contentEncoding :: Maybe ByteString }

data KVOpts = KVOpts { bucketType :: BucketType
                     , bucket :: ByteString
                     , key :: ByteString }
