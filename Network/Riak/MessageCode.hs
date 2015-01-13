{-# LANGUAGE DeriveDataTypeable #-}
module Network.Riak.MessageCode where

import Control.Exception
import Data.Serialize
import Data.Typeable
import Data.Word

data MessageCode = ErrorResponseCode
                 | PingRequestCode
                 | PingResponseCode
                 | GetRequestCode
                 | GetResponseCode
                 | PutRequestCode
                 | PutResponseCode

data InvalidMessageCode = InvalidMessageCode Word8 deriving (Typeable, Show)
instance Exception InvalidMessageCode

messageCode :: Word8 -> MessageCode
messageCode 0 = ErrorResponseCode
messageCode 1 = PingRequestCode
messageCode 2 = PingResponseCode
messageCode 9 = GetRequestCode
messageCode 10 = GetResponseCode
messageCode 11 = PutRequestCode
messageCode 12 = PutResponseCode
messageCode code = throw (InvalidMessageCode code)

messageCodeRep :: MessageCode -> Word8
messageCodeRep ErrorResponseCode = 0
messageCodeRep PingRequestCode = 1
messageCodeRep PingResponseCode = 2
messageCodeRep GetRequestCode = 9
messageCodeRep GetResponseCode = 10
messageCodeRep PutRequestCode = 11
messageCodeRep PutResponseCode = 12

getMessageCode :: Get MessageCode
getMessageCode = fmap messageCode getWord8

putMessageCode :: MessageCode -> Put
putMessageCode = putWord8 . messageCodeRep
