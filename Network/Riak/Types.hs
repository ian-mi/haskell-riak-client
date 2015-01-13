module Network.Riak.Types where

import Data.ByteString
import Data.Time.Clock

data BucketType = Default | BucketType ByteString
newtype Bucket = Bucket {bucket :: ByteString}
newtype Key = Key {key :: ByteString}

newtype VClock = VClock {vclock :: Maybe ByteString}

data Metadata = Metadata { contentType :: Maybe ByteString
                         , charset :: Maybe ByteString
                         , contentEncoding :: Maybe ByteString }
