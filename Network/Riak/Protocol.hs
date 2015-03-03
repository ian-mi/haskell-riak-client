{-# LANGUAGE DeriveGeneric, DataKinds #-}

module Network.Riak.Protocol where

import Data.ByteString
import Data.ProtocolBuffers
import Data.Text
import Data.Word
import GHC.Generics

data RpbErrorResp = RpbErrorResp {errmsg :: Required 1 (Value Text), errcode :: Required 2 (Value Word32)} deriving Generic
instance Encode RpbErrorResp
instance Decode RpbErrorResp

data RpbPingReq = RpbPingReq deriving Generic
instance Encode RpbPingReq
instance Decode RpbPingReq

data RpbPingResp = RpbPingResp deriving Generic
instance Encode RpbPingResp
instance Decode RpbPingResp

data RpbGetReq = RpbGetReq { getBucket :: Required 1 (Value ByteString)
                           , getKey :: Required 2 (Value ByteString)
                           , r :: Optional 3 (Value Word32)
                           , pr :: Optional 4 (Value Word32)
                           , basic_quorom :: Optional 5 (Value Bool)
                           , notfound_ok :: Optional 6 (Value Bool)
                           , if_modified :: Optional 7 (Value ByteString)
                           , getHead :: Optional 8 (Value Bool)
                           , deleted_vclock :: Optional 9 (Value Bool)
                           , getTimeout :: Optional 10 (Value Word32)
                           , getSloppyQuorom :: Optional 11 (Value Bool)
                           , n_val :: Optional 12 (Value Word32)
                           , getBucketType :: Optional 13 (Value ByteString) } deriving Generic
instance Encode RpbGetReq
instance Decode RpbGetReq

data RpbGetResp = RpbGetResp { getContent :: Repeated 1 (Message RpbContent)
                             , getVclock :: Optional 2 (Value ByteString)
                             , unchanged :: Optional 3 (Value Bool) } deriving Generic
instance Encode RpbGetResp
instance Decode RpbGetResp

data RpbPutReq = RpbPutReq { putBucket :: Required 1 (Value ByteString)
                           , putKey :: Optional 2 (Value ByteString)
                           , putVclock :: Optional 3 (Value ByteString)
                           , putContent :: Required 4 (Message RpbContent)
                           , w :: Optional 5 (Value Word32)
                           , dw :: Optional 6 (Value Word32)
                           , return_body :: Optional 7 (Value Bool)
                           , pw :: Optional 8 (Value Word32)
                           , if_not_modified :: Optional 9 (Value Bool)
                           , if_none_match :: Optional 10 (Value Bool)
                           , return_head :: Optional 11 (Value Bool)
                           , putTimeout :: Optional 12 (Value Word32)
                           , asis :: Optional 13 (Value Word32)
                           , putSloppyQuorom :: Optional 14 (Value Bool)
                           , nval :: Optional 15 (Value Word32)
                           , putBucketType :: Optional 16 (Value ByteString) } deriving Generic
instance Encode RpbPutReq
instance Decode RpbPutReq

data RpbPutResp = RpbPutResp { putRespContents :: Repeated 1 (Message RpbContent)
                             , putRespVclock :: Optional 2 (Value ByteString)
                             , putRespKey :: Optional 3 (Value ByteString) } deriving Generic
instance Encode RpbPutResp
instance Decode RpbPutResp

data RpbContent = RpbContent { value :: Required 1 (Value ByteString)
                             , content_type :: Optional 2 (Value ByteString)
                             , charset :: Optional 3 (Value ByteString)
                             , content_encoding :: Optional 4 (Value ByteString)
                             , vtag :: Optional 5 (Value ByteString)
                             , links :: Repeated 6 (Message RpbLink)
                             , last_mod :: Optional 7 (Value Word32)
                             , last_mod_usecs :: Optional 8 (Value Word32)
                             , usermeta :: Repeated 9 (Message RpbPair)
                             , indexes :: Repeated 10 (Message RpbPair)
                             , deleted :: Optional 11 (Value Bool) } deriving Generic
instance Encode RpbContent
instance Decode RpbContent

data RpbPair = RpbPair { pairKey :: Required 1 (Value ByteString)
                       , pairValue :: Optional 2 (Value ByteString) } deriving Generic
instance Encode RpbPair
instance Decode RpbPair

data RpbLink = RpbLink { linkBucket :: Optional 1 (Value ByteString)
                       , linkKey :: Optional 2 (Value ByteString)
                       , tag :: Optional 3 (Value ByteString) } deriving Generic
instance Encode RpbLink
instance Decode RpbLink
