{-# LANGUAGE RecordWildCards #-}
module Network.Riak.Request where

import Network.Riak.Message
import Network.Riak.Types

import Data.ByteString

newtype ReturnBody = ReturnBody {returnBody :: Bool}

getRequest :: BucketType -> Bucket -> Key -> Message
getRequest bucketType Bucket{..} Key{..} = GetRequest { getBucket = bucket
                                                      , getKey = key
                                                      , r = Nothing
                                                      , pr = Nothing
                                                      , basic_quorom = Nothing
                                                      , notfound_ok = Nothing
                                                      , if_modified = Nothing
                                                      , getHead = Nothing
                                                      , deleted_vclock = Nothing
                                                      , getTimeout = Nothing
                                                      , getSloppyQuorom = Nothing
                                                      , n_val = Nothing
                                                      , getBucketType = encodeBucketType bucketType }

putRequest :: ReturnBody -> VClock -> BucketType -> Bucket -> Key -> ByteString -> Metadata -> Message
putRequest ReturnBody{..} VClock{..} bucketType Bucket{..} Key{..} value metadata =
  PutRequest { putBucket = bucket
             , putKey = Just key
             , putVclock = vclock
             , putContent = encodeContent value metadata
             , w = Nothing
             , dw = Nothing
             , return_body = Just returnBody
             , pw = Nothing
             , if_not_modified = Nothing
             , if_none_match = Nothing
             , return_head = Nothing
             , putTimeout = Nothing
             , asis = Nothing
             , putSloppyQuorom = Nothing
             , nval = Nothing
             , putBucketType = encodeBucketType bucketType }

encodeBucketType :: BucketType -> Maybe ByteString
encodeBucketType Default = Nothing
enocdeBucketType (BucketType bucketType) = Just bucketType

encodeContent :: ByteString -> Metadata -> Content
encodeContent value Metadata{..} = Content{ value = value
                                          , content_type = contentType
                                          , charset = charset
                                          , content_encoding = contentEncoding
                                          , vtag = Nothing
                                          , links = []
                                          , last_mod = Nothing
                                          , last_mod_usecs = Nothing
                                          , usermeta = []
                                          , indexes = []
                                          , deleted = Nothing }
