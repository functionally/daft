{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeOperators        #-}


module Data.Daft.Vinyl.FieldCube.Aeson (
  toArray
, fromArray
, parseArray
) where


import Data.Aeson.Types (FromJSON(..), Parser, ToJSON(..), Value, parseMaybe)
import Data.Daft.DataCube (fromTableM, toKnownTable)
import Data.Daft.Vinyl.FieldCube (FieldCube)
import Data.Daft.Vinyl.FieldRec.Aeson ()
import Data.Vinyl.Core ((<+>))
import Data.Vinyl.Derived (FieldRec)
import Data.Vinyl.TypeLevel (type (++))


toArray :: (Ord (FieldRec ks), ToJSON (FieldRec (ks ++ vs))) => FieldCube ks vs -> [Value]
toArray = toKnownTable $ (toJSON .) . (<+>)


fromArray :: forall ks vs . (Ord (FieldRec ks), FromJSON (FieldRec ks), FromJSON (FieldRec vs)) => [Value] -> Maybe (FieldCube ks vs)
fromArray = parseMaybe parseArray


parseArray :: forall ks vs . (Ord (FieldRec ks), FromJSON (FieldRec ks), FromJSON (FieldRec vs)) => [Value] -> Parser (FieldCube ks vs)
parseArray = fromTableM parseJSON parseJSON
