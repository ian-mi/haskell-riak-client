{-# LANGUAGE DisambiguateRecordFields, NamedFieldPuns, DeriveDataTypeable #-}
module Network.Riak.Response where

import Network.Riak.Message
import Network.Riak.Types

import Control.Exception
import Data.ByteString
import Data.Ratio
import Data.Text
import Data.Time
import Data.Time.Clock.POSIX
import Data.Typeable
import Data.Word

data ResponseException = UnexpectedResponse Message
                       | ConnectionClosed
                       | RiakError { errMsg :: Text, errCode :: Int } deriving (Show, Typeable)

instance Exception ResponseException

pingResponse :: Message -> ()
pingResponse PingResponse = ()
pingResponse m = throw (UnexpectedResponse m)

getResponse :: Message -> (VClock, [(ByteString, Metadata, Maybe UTCTime)])
getResponse GetResponse {getContent, getVclock} = (VClock getVclock, fmap decodeContent getContent)
getResponse ErrorResponse {errmsg, errcode} = throw (RiakError {errMsg = errmsg, errCode = errcode})
getResponse m = throw (UnexpectedResponse m)

putResponse :: Message -> (VClock, [(ByteString, Metadata, Maybe UTCTime)])
putResponse PutResponse {putRespContents, putRespVclock} = (VClock putRespVclock, fmap decodeContent putRespContents)

deleteResponse :: Message -> (VClock, ())
deleteResponse DeleteResponse = (VClock Nothing, ())
deleteResponse ErrorResponse {errmsg, errcode} = throw RiakError {errMsg = errmsg, errCode = errcode}
deleteResponse m = throw (UnexpectedResponse m)

decodeContent :: Content -> (ByteString, Metadata, Maybe UTCTime)
decodeContent Content { value, content_type, charset, content_encoding, last_mod, last_mod_usecs } = 
  ( value
  , Metadata { contentType = content_type
             , charset = charset
             , contentEncoding = content_encoding }
  , fmap (decodeLastModified last_mod_usecs) last_mod )

decodeLastModified :: Maybe Word32 -> Word32 -> UTCTime
decodeLastModified usecs secs = maybe utcSecs (flip addUTCTime utcSecs . realToFrac . (% 10^6)) usecs
  where utcSecs = posixSecondsToUTCTime (fromIntegral secs)
