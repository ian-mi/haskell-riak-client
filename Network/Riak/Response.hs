{-# LANGUAGE DeriveDataTypeable, NamedFieldPuns, DisambiguateRecordFields #-}
module Network.Riak.Response where

import Network.Riak.Message
import Network.Riak.Types

import Control.Monad.Morph
import Control.Exception
import Control.Monad.Catch
import Data.ByteString
import Data.Conduit
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

awaitThrow :: MonadThrow m => Consumer a m a
awaitThrow = await >>= maybe (throwM ConnectionClosed) return

getResponse :: MonadThrow m => (Message -> m a) -> Sink Message m a
getResponse f = awaitThrow >>= lift . f

pingResponse :: MonadThrow m => Message -> m ()
pingResponse PingResponse = return ()
pingResponse m = throwM (UnexpectedResponse m)

fetchResponse :: MonadThrow m => Message -> m ([(ByteString, Metadata, Maybe UTCTime)], VClock)
fetchResponse GetResponse { getContent, getVclock } = return (fmap decodeContent getContent, VClock getVclock)
fetchResponse ErrorResponse { errmsg, errcode } = throwM (RiakError { errMsg = errmsg, errCode = errcode })
fetchResponse m = throwM (UnexpectedResponse m)

putResponse :: MonadThrow m => Message -> m ([(ByteString, Metadata, Maybe UTCTime)], VClock)
putResponse PutResponse { putRespContents, putRespVclock } = return (fmap decodeContent putRespContents, VClock putRespVclock)
putResponse ErrorResponse { errmsg, errcode } = throwM (RiakError { errMsg = errmsg, errCode = errcode })
putResponse m = throwM (UnexpectedResponse m)

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
