{-# LANGUAGE GADTs, DeriveDataTypeable, DisambiguateRecordFields, NamedFieldPuns, RecordWildCards #-}
module Network.Riak.Message (Message(..), Content(..), Link(..), Pair(..), parseMessages, putMessage) where

import Network.Riak.Protocol
import Network.Riak.MessageCode

import Control.Applicative
import Control.Monad.Catch
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Conduit
import Data.Conduit.Cereal
import qualified Data.Conduit.List as CL
import Data.Typeable
import qualified Data.ProtocolBuffers as PB
import Data.Serialize
import Data.Text
import Data.Word

data Message =
    ErrorResponse { errmsg :: Text
                  , errcode :: Int }
  | PingRequest
  | PingResponse
  | GetRequest { getBucket :: BS.ByteString
               , getKey :: BS.ByteString
               , getR :: Maybe Int
               , getPR :: Maybe Int
               , basic_quorom :: Maybe Bool
               , notfound_ok :: Maybe Bool
               , if_modified :: Maybe BS.ByteString
               , getHead :: Maybe Bool
               , deleted_vclock :: Maybe Bool
               , getTimeout :: Maybe Int
               , getSloppyQuorom :: Maybe Bool
               , n_val :: Maybe Int
               , getBucketType :: Maybe BS.ByteString }
  | GetResponse { getContent :: [Content]
                , getVclock :: Maybe BS.ByteString
                , unchanged :: Maybe Bool }
  | PutRequest { putBucket :: BS.ByteString
               , putKey :: Maybe BS.ByteString
               , putVclock :: Maybe BS.ByteString
               , putContent :: Content
               , putW :: Maybe Int
               , putDW :: Maybe Int
               , return_body :: Maybe Bool
               , putPW :: Maybe Int
               , if_not_modified :: Maybe Bool
               , if_none_match :: Maybe Bool
               , return_head :: Maybe Bool
               , putTimeout :: Maybe Int
               , asis :: Maybe Int
               , putSloppyQuorom :: Maybe Bool
               , putNVal :: Maybe Int
               , putBucketType :: Maybe BS.ByteString }
  | PutResponse { putRespContents :: [Content]
                , putRespVclock :: Maybe BS.ByteString
                , putRespKey :: Maybe BS.ByteString }
  | DeleteRequest { delBucket :: BS.ByteString
                  , delKey :: BS.ByteString
                  , rw :: Maybe Int
                  , delVclock :: Maybe BS.ByteString
                  , delR :: Maybe Int
                  , delW :: Maybe Int
                  , delPR :: Maybe Int
                  , delPW :: Maybe Int
                  , delDW :: Maybe Int
                  , delTimeout :: Maybe Int
                  , delSloppyQuorom :: Maybe Bool
                  , delNVal :: Maybe Int
                  , delBucketType :: Maybe BS.ByteString }
  |  DeleteResponse deriving Show

data Content = Content { value :: BS.ByteString
                       , content_type :: Maybe BS.ByteString
                       , charset :: Maybe BS.ByteString
                       , content_encoding :: Maybe BS.ByteString
                       , vtag :: Maybe BS.ByteString
                       , links :: [Link]
                       , last_mod :: Maybe Word32
                       , last_mod_usecs :: Maybe Word32
                       , usermeta :: [Pair]
                       , indexes :: [Pair]
                       , deleted :: Maybe Bool } deriving Show

data Link = Link { linkBucket :: Maybe BS.ByteString
                 , linkKey :: Maybe BS.ByteString
                 , tag :: Maybe BS.ByteString } deriving Show

data Pair = Pair { pairKey :: BS.ByteString, pairValue :: Maybe BS.ByteString } deriving Show

data Decoder where Decoder :: PB.Decode a => (a -> Message) -> Decoder
data Encoder where Encoder :: PB.Encode a => a -> Encoder

data ParseException = ParseException String deriving (Show, Typeable)
instance Exception ParseException

parseMessages :: MonadThrow m => Conduit BS.ByteString m Message
parseMessages = conduitGet (getMessageLen >>= getLazyByteString . fromIntegral) =$= getMessages

getMessages :: MonadThrow m => Conduit LBS.ByteString m Message
getMessages = CL.mapM (either (throwM . ParseException) return . runGetLazy getMessage)

getMessage :: Get Message
getMessage = getMessageCode >>= getMessageByCode

getMessageLen :: Get Int
getMessageLen = fmap fromIntegral getWord32be

getMessageByCode :: MessageCode -> Get Message
getMessageByCode msg | Decoder dec <- decoder msg = fmap dec PB.decodeMessage

decoder :: MessageCode -> Decoder
decoder ErrorResponseCode = Decoder decodeErrorResponse
decoder PingRequestCode = Decoder decodePingRequest
decoder PingResponseCode = Decoder decodePingResponse
decoder GetRequestCode = Decoder decodeGetRequest
decoder GetResponseCode = Decoder decodeGetResponse
decoder PutRequestCode = Decoder decodePutRequest
decoder PutResponseCode = Decoder decodePutResponse
decoder DeleteRequestCode = Decoder decodeDeleteRequest
decoder DeleteResponseCode = Decoder decodeDeleteResponse

decodeErrorResponse :: RpbErrorResp -> Message
decodeErrorResponse RpbErrorResp { errmsg, errcode } = ErrorResponse { errmsg = PB.getField errmsg, errcode = fromIntegral (PB.getField errcode) }

decodePingRequest :: RpbPingReq -> Message
decodePingRequest = const PingRequest

decodePingResponse :: RpbPingResp -> Message
decodePingResponse = const PingResponse

decodeGetRequest :: RpbGetReq -> Message
decodeGetRequest RpbGetReq {..} = 
  GetRequest { getBucket = PB.getField getBucket
             , getKey = PB.getField getKey
             , getR = fmap fromIntegral (PB.getField getR)
             , getPR = fmap fromIntegral (PB.getField getPR)
             , basic_quorom = PB.getField basic_quorom
             , notfound_ok = PB.getField notfound_ok
             , if_modified = PB.getField if_modified
             , getHead = PB.getField getHead
             , deleted_vclock = PB.getField deleted_vclock
             , getTimeout = fmap fromIntegral (PB.getField getTimeout)
             , getSloppyQuorom = PB.getField getSloppyQuorom
             , n_val = fmap fromIntegral (PB.getField n_val)
             , getBucketType = PB.getField getBucketType }

decodeGetResponse :: RpbGetResp -> Message
decodeGetResponse RpbGetResp { getContent, getVclock, unchanged } =
  GetResponse { getContent = fmap decodeContent (PB.getField getContent), getVclock = PB.getField getVclock, unchanged = PB.getField unchanged }

decodePutRequest :: RpbPutReq -> Message
decodePutRequest RpbPutReq {..} = PutRequest { putBucket = PB.getField putBucket
                                             , putKey = PB.getField putKey
                                             , putVclock = PB.getField putVclock
                                             , putContent = decodeContent (PB.getField putContent)
                                             , putW = fmap fromIntegral (PB.getField putW)
                                             , putDW = fmap fromIntegral (PB.getField putDW)
                                             , return_body = PB.getField return_body
                                             , putPW = fmap fromIntegral (PB.getField putPW)
                                             , if_not_modified = PB.getField if_not_modified
                                             , if_none_match = PB.getField if_none_match
                                             , return_head = PB.getField return_head
                                             , putTimeout = fmap fromIntegral (PB.getField putTimeout)
                                             , asis = fmap fromIntegral (PB.getField asis)
                                             , putSloppyQuorom = PB.getField putSloppyQuorom
                                             , putNVal = fmap fromIntegral (PB.getField putNVal)
                                             , putBucketType = PB.getField putBucketType }

decodePutResponse :: RpbPutResp -> Message
decodePutResponse RpbPutResp {..} = PutResponse { putRespContents = fmap decodeContent (PB.getField putRespContents)
                                                , putRespVclock = PB.getField putRespVclock
                                                , putRespKey = PB.getField putRespKey }

decodeDeleteRequest :: RpbDelReq -> Message
decodeDeleteRequest RpbDelReq {..} = DeleteRequest { delBucket = PB.getField delBucket
                                                   , delKey = PB.getField delKey
                                                   , rw = fmap fromIntegral (PB.getField rw)
                                                   , delVclock = PB.getField delVclock
                                                   , delR = fmap fromIntegral (PB.getField delR)
                                                   , delW = fmap fromIntegral (PB.getField delW)
                                                   , delPR = fmap fromIntegral (PB.getField delPR)
                                                   , delPW = fmap fromIntegral (PB.getField delPW)
                                                   , delDW = fmap fromIntegral (PB.getField delDW)
                                                   , delTimeout = fmap fromIntegral (PB.getField delTimeout)
                                                   , delSloppyQuorom = PB.getField delSloppyQuorom
                                                   , delNVal = fmap fromIntegral (PB.getField delNVal)
                                                   , delBucketType = PB.getField delBucketType }

decodeDeleteResponse :: RpbDelResp -> Message
decodeDeleteResponse RpbDelResp = DeleteResponse

decodeContent :: RpbContent -> Content
decodeContent RpbContent { value, content_type, charset, content_encoding, vtag, links, last_mod, last_mod_usecs, usermeta, indexes, deleted } =
  Content { value = PB.getField value
          , content_type = PB.getField content_type
          , charset = PB.getField charset
          , content_encoding = PB.getField content_encoding
          , vtag = PB.getField vtag
          , links = fmap decodeLink (PB.getField links)
          , last_mod = PB.getField last_mod
          , last_mod_usecs = PB.getField last_mod_usecs
          , usermeta = fmap decodePair (PB.getField usermeta)
          , indexes = fmap decodePair (PB.getField indexes)
          , deleted = PB.getField deleted }

decodeLink :: RpbLink -> Link
decodeLink RpbLink { linkBucket, linkKey, tag } = 
  Link { linkBucket = PB.getField linkBucket, linkKey = PB.getField linkKey, tag = PB.getField tag }

decodePair :: RpbPair -> Pair
decodePair RpbPair { pairKey, pairValue } = Pair { pairKey = PB.getField pairKey, pairValue = PB.getField pairValue }

putMessage :: Message -> Put
putMessage msg = putMessageLen (LBS.length body) >> putLazyByteString body
  where body = runPutLazy (putMessageBody msg)

putMessageLen :: Integral a => a -> Put
putMessageLen = putWord32be . fromIntegral

putMessageBody :: Message -> Put
putMessageBody msg | (code, Encoder enc) <- encoder msg = putMessageCode code >> PB.encodeMessage enc

encoder :: Message -> (MessageCode, Encoder)
encoder ErrorResponse { errmsg, errcode } =
  (ErrorResponseCode, Encoder RpbErrorResp { errmsg = PB.putField errmsg
                                           , errcode = PB.putField (fromIntegral errcode) })
encoder PingRequest = (PingRequestCode, Encoder RpbPingReq)
encoder PingResponse = (PingResponseCode, Encoder RpbPingResp)
encoder GetRequest {..} = (GetRequestCode, Encoder RpbGetReq { getBucket = PB.putField getBucket
                                                             , getKey = PB.putField getKey
                                                             , getR = PB.putField (fmap fromIntegral getR)
                                                             , getPR = PB.putField (fmap fromIntegral getPR)
                                                             , basic_quorom = PB.putField basic_quorom
                                                             , notfound_ok = PB.putField notfound_ok
                                                             , if_modified = PB.putField if_modified
                                                             , getHead = PB.putField getHead
                                                             , deleted_vclock = PB.putField deleted_vclock
                                                             , getTimeout = PB.putField (fmap fromIntegral getTimeout)
                                                             , getSloppyQuorom = PB.putField getSloppyQuorom
                                                             , n_val = PB.putField (fmap fromIntegral n_val)
                                                             , getBucketType = PB.putField getBucketType })
encoder (GetResponse { getContent, getVclock, unchanged }) = 
  (GetResponseCode, Encoder RpbGetResp { getContent = PB.putField (fmap encodeContent getContent)
                                       , getVclock = PB.putField getVclock
                                       , unchanged = PB.putField unchanged })
encoder (PutRequest {..}) = (PutRequestCode, Encoder RpbPutReq { putBucket = PB.putField putBucket
                                                               , putKey = PB.putField putKey
                                                               , putVclock = PB.putField putVclock
                                                               , putContent = PB.putField (encodeContent putContent)
                                                               , putW = PB.putField (fmap fromIntegral putW)
                                                               , putDW = PB.putField (fmap fromIntegral putDW)
                                                               , return_body = PB.putField return_body
                                                               , putPW = PB.putField (fmap fromIntegral putPW)
                                                               , if_not_modified = PB.putField if_not_modified
                                                               , if_none_match = PB.putField if_none_match
                                                               , return_head = PB.putField return_head
                                                               , putTimeout = PB.putField (fmap fromIntegral putTimeout)
                                                               , asis = PB.putField (fmap fromIntegral asis)
                                                               , putSloppyQuorom = PB.putField putSloppyQuorom
                                                               , putNVal = PB.putField (fmap fromIntegral putNVal)
                                                               , putBucketType = PB.putField putBucketType })
encoder (PutResponse {..}) = (PutResponseCode, Encoder RpbPutResp { putRespContents = PB.putField (fmap encodeContent putRespContents)
                                                                  , putRespVclock = PB.putField putRespVclock
                                                                  , putRespKey = PB.putField putRespKey })
encoder (DeleteRequest {..}) = (DeleteRequestCode, Encoder RpbDelReq { delBucket = PB.putField delBucket
                                                                     , delKey = PB.putField delKey
                                                                     , rw = PB.putField (fmap fromIntegral rw)
                                                                     , delVclock = PB.putField delVclock
                                                                     , delR = PB.putField (fmap fromIntegral delR)
                                                                     , delW = PB.putField (fmap fromIntegral delW)
                                                                     , delPR = PB.putField (fmap fromIntegral delPR)
                                                                     , delPW = PB.putField (fmap fromIntegral delPW)
                                                                     , delDW = PB.putField (fmap fromIntegral delDW)
                                                                     , delTimeout = PB.putField (fmap fromIntegral delTimeout)
                                                                     , delSloppyQuorom = PB.putField delSloppyQuorom
                                                                     , delNVal = PB.putField (fmap fromIntegral delNVal)
                                                                     , delBucketType = PB.putField delBucketType })
encoder DeleteResponse = (DeleteResponseCode, Encoder RpbDelResp)

encodeContent :: Content -> RpbContent
encodeContent Content {value, content_type, charset, content_encoding, vtag, links, last_mod, last_mod_usecs, usermeta, indexes, deleted} = 
  RpbContent { value = PB.putField value
             , content_type = PB.putField content_type
             , charset = PB.putField charset
             , content_encoding = PB.putField content_encoding
             , vtag = PB.putField vtag
             , links = PB.putField (fmap encodeLink links)
             , last_mod = PB.putField last_mod
             , last_mod_usecs = PB.putField last_mod_usecs
             , usermeta = PB.putField (fmap encodePair usermeta)
             , indexes = PB.putField (fmap encodePair indexes)
             , deleted = PB.putField deleted }

-- encodeLastModified :: UTCTime -> (Word32, Word32)
-- encodeLastModified utcTime = (secs, usecs)
--   where (secs, rem) = properFraction (utcTimeToPOSIXSeconds utcTime)
--         usecs = truncate (rem * fromIntegral (10^6))

encodeLink :: Link -> RpbLink
encodeLink Link {linkBucket, linkKey, tag} = RpbLink {linkBucket = PB.putField linkBucket, linkKey = PB.putField linkKey, tag = PB.putField tag}

encodePair :: Pair -> RpbPair
encodePair Pair {pairKey, pairValue} = RpbPair {pairKey = PB.putField pairKey, pairValue = PB.putField pairValue}
