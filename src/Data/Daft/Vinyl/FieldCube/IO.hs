{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}


module Data.Daft.Vinyl.FieldCube.IO (
-- * Input
  readFieldCube
, readFieldCubeFile
, readFieldCubeSource
, readFieldCubeSource'
-- * Output
, showFieldCube
, writeFieldCubeFile
, writeFieldCubeSource
) where


import Control.Monad.Except (MonadError, MonadIO, throwError)
import Data.Daft.DataCube (fromFunction)
import Data.Daft.Source (DataSource(..))
import Data.Daft.TypeLevel (Union)
import Data.Daft.Vinyl.FieldCube (FieldCube)
import Data.Daft.Vinyl.FieldRec (Labeled)
import Data.Daft.Vinyl.FieldRec.Instances ()
import Data.Daft.Vinyl.FieldRec.IO (ReadFieldRec, ShowFieldRec, readFieldRecs, readFieldRecFile, readFieldRecSource, showFieldRecs, writeFieldRecFile, writeFieldRecSource)
import Data.Default.Util (Unknown(..))
import Data.List.Util.Listable (fromTabbeds)
import Data.String (IsString(..))
import Data.String.ToString (ToString(..))
import Data.Vinyl.Core ((<+>))
import Data.Vinyl.Derived (FieldRec)
import Data.Vinyl.Lens (type (⊆), rcast)
import Data.Vinyl.TypeLevel (type (++))

import qualified Data.Daft.DataCube as C (fromTable, toKnownTable)


readFieldCube :: forall ks vs s e m . (ks ⊆ Union ks vs, vs ⊆ Union ks vs, Ord (FieldRec ks), Eq s, IsString s, ToString s, IsString e, MonadError e m, Labeled (FieldRec (Union ks vs)), ReadFieldRec (Union ks vs)) => [[s]] -> m (FieldCube ks vs)
readFieldCube =
  fmap (C.fromTable (rcast :: FieldRec (Union ks vs) -> FieldRec ks) (rcast :: FieldRec (Union ks vs) -> FieldRec vs))
    . readFieldRecs


readFieldCubeFile :: forall ks vs e m . (ks ⊆ Union ks vs, vs ⊆ Union ks vs, Ord (FieldRec ks), IsString e, MonadError e m, MonadIO m, Labeled (FieldRec (Union ks vs)), ReadFieldRec (Union ks vs)) => FilePath -> m (FieldCube ks vs)
readFieldCubeFile =
  fmap (C.fromTable (rcast :: FieldRec (Union ks vs) -> FieldRec ks) (rcast :: FieldRec (Union ks vs) -> FieldRec vs)) 
    . readFieldRecFile


readFieldCubeSource :: forall ks vs e m a . (ks ⊆ Union ks vs, vs ⊆ Union ks vs, Ord (FieldRec ks), IsString e, MonadError e m, MonadIO m, Labeled (FieldRec (Union ks vs)), ReadFieldRec (Union ks vs)) => DataSource a -> m (FieldCube ks vs)
readFieldCubeSource =
  fmap (C.fromTable (rcast :: FieldRec (Union ks vs) -> FieldRec ks) (rcast :: FieldRec (Union ks vs) -> FieldRec vs)) 
    . readFieldRecSource


readFieldCubeSource' :: forall ks vs e m a . (ks ⊆ Union ks vs, vs ⊆ Union ks vs, Ord (FieldRec ks), Unknown (FieldRec vs), IsString e, MonadError e m, MonadIO m, Labeled (FieldRec (Union ks vs)), ReadFieldRec (Union ks vs)) => DataSource a -> m (FieldCube ks vs)
readFieldCubeSource' FileData{..}    = fmap (C.fromTable (rcast :: FieldRec (Union ks vs) -> FieldRec ks) (rcast :: FieldRec (Union ks vs) -> FieldRec vs)) $ readFieldRecFile filePath
readFieldCubeSource' TextData{..}    = fmap (C.fromTable (rcast :: FieldRec (Union ks vs) -> FieldRec ks) (rcast :: FieldRec (Union ks vs) -> FieldRec vs)) . readFieldRecs $ fromTabbeds parsableText
readFieldCubeSource' BuiltinData{..} = throwError "Cannot read records from built-in data source."
readFieldCubeSource' NoData          = return . fromFunction . const $ return unknown 


showFieldCube :: forall ks vs . (Ord (FieldRec ks), Labeled (FieldRec (ks ++ vs)), ShowFieldRec (ks ++ vs)) => FieldCube ks vs -> [[String]]
showFieldCube = showFieldRecs . C.toKnownTable ((<+>) :: FieldRec ks -> FieldRec vs -> FieldRec (ks ++ vs))


writeFieldCubeFile :: forall ks vs e m . (Ord (FieldRec ks), Labeled (FieldRec (ks ++ vs)), ShowFieldRec (ks ++ vs), IsString e, MonadError e m, MonadIO m) => FilePath -> FieldCube ks vs -> m ()
writeFieldCubeFile = (. C.toKnownTable ((<+>) :: FieldRec ks -> FieldRec vs -> FieldRec (ks ++ vs))) . writeFieldRecFile


writeFieldCubeSource :: forall ks vs e m a . (Ord (FieldRec ks), Labeled (FieldRec (ks ++ vs)), ShowFieldRec (ks ++ vs), IsString e, MonadError e m, MonadIO m) => DataSource a -> FieldCube ks vs -> m (Maybe String)
writeFieldCubeSource = (. C.toKnownTable ((<+>) :: FieldRec ks -> FieldRec vs -> FieldRec (ks ++ vs))) . writeFieldRecSource
