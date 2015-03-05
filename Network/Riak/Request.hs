{-# LANGUAGE RecordWildCards #-}
module Network.Riak.Request where

import Network.Riak.Message
import Network.Riak.Types

import Data.ByteString

newtype ReturnBody = ReturnBody {returnBody :: Bool}

getRequest :: BucketType -> Bucket -> Key -> Message
getRequest bucketType Bucket{..} Key{..} = GetRequest { getBucket = bucket
                                                      , getKey = key
                                                      , getR = Nothing
                                                      , getPR = Nothing
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
             , putW = Nothing
             , putDW = Nothing
             , return_body = Just returnBody
             , putPW = Nothing
             , if_not_modified = Nothing
             , if_none_match = Nothing
             , return_head = Nothing
             , putTimeout = Nothing
             , asis = Nothing
             , putSloppyQuorom = Nothing
             , putNVal = Nothing
             , putBucketType = encodeBucketType bucketType }

deleteRequest :: VClock -> BucketType -> Bucket -> Key -> Message
deleteRequest VClock{..} bucketType Bucket{..} Key{..} =
  DeleteRequest { delBucket = bucket
                , delKey = key
                , rw = Nothing
                , delVclock = vclock
                , delR = Nothing
                , delW = Nothing
                , delPR = Nothing
                , delPW = Nothing
                , delDW = Nothing
                , delTimeout = Nothing
                , delSloppyQuorom = Nothing
                , delNVal = Nothing
                , delBucketType = encodeBucketType bucketType }

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
