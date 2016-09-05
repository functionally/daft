{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators    #-}


module Data.Daft.Vinyl.FieldCube (
-- * Types
  type (↝)
, FieldCube
, FieldGregator
-- * Conversion
, fromRecords
, toRecords
, toKnownRecords
-- * Evaluation
, (!)
-- * Selection, projection, and aggregation
, σ
, π
, κ
, ω
, ρ
-- * Joins
, (⋈)
, (⋉)
, (▷)
, (×)
{- TODO: Implement these:
-- * Input/output
, readFieldCube
, readFieldCubeFile
, readFiledCubeSource
, showFieldCube
, writeFieldCubeFile
, writeFieldCubeSource
-}
) where


import Control.Applicative (liftA2)
import Data.Daft.DataCube (DataCube)
import Data.Daft.TypeLevel (Intersection)
import Data.Daft.Vinyl.TypeLevel (RDistinct, RJoin(rjoin), RUnion(runion))
import Data.Maybe (fromMaybe)
import Data.Set (Set)
import Data.Vinyl.Core ((<+>))
import Data.Vinyl.Derived (FieldRec)
import Data.Vinyl.Lens (type (⊆), rcast)
import Data.Vinyl.TypeLevel (type (++))

import qualified Data.Daft.DataCube as C (Gregator, Joiner(Joiner), aggregateWithKey, antijoin, evaluate, fromKnownKeys, fromTable, join, knownKeys, projectWithKey, reify, selectWithKey, semijoin, toKnownTable, toTable)
import qualified Data.Set as S (fromDistinctAscList, map, toAscList)


type ks ↝ vs = FieldCube ks vs


type FieldCube ks vs = DataCube (FieldRec ks) (FieldRec vs)


fromRecords :: (ks ⊆ as, vs ⊆ as, RUnion ks vs as, Ord (FieldRec ks)) => [FieldRec as] -> FieldCube ks vs
fromRecords = C.fromTable rcast rcast


toRecords :: (Ord (FieldRec ks), RUnion ks vs as) => [FieldRec ks] -> FieldCube ks vs -> [FieldRec as]
toRecords = C.toTable runion


toKnownRecords :: (Ord (FieldRec ks), RUnion ks vs as) => FieldCube ks vs -> [FieldRec as]
toKnownRecords = C.toKnownTable runion


(!) :: Ord (FieldRec ks) => FieldCube ks vs -> FieldRec ks -> FieldRec vs
(!) = (fromMaybe (error "FieldCube: key not found.") .) . C.evaluate


σ :: (FieldRec ks -> FieldRec vs -> Bool) -> FieldCube ks vs -> FieldCube ks vs
σ = C.selectWithKey


π :: (FieldRec ks -> FieldRec vs -> FieldRec ws) -> FieldCube ks vs -> FieldCube ks ws
π = C.projectWithKey


ρ :: Ord (FieldRec ks) => Set (FieldRec ks) -> FieldCube ks vs -> FieldCube ks vs
ρ = C.reify


type FieldGregator as bs = C.Gregator (FieldRec as) (FieldRec bs)


κ :: (ks ⊆ ks0, ks' ⊆ ks, Ord (FieldRec ks), Ord (FieldRec ks')) => Set (FieldRec ks0) -> (FieldRec ks' -> [FieldRec vs] -> FieldRec vs') -> FieldCube ks vs -> FieldCube ks' vs'
κ = C.aggregateWithKey . C.fromKnownKeys rcast . S.map rcast


ω :: (ks' ⊆ ks, Ord (FieldRec ks')) => FieldCube ks vs -> Set (FieldRec ks')
ω = S.map rcast . C.knownKeys


(⋈) :: (Eq (FieldRec (Intersection kLeft kRight)), Intersection kLeft kRight ⊆ kLeft, Intersection kLeft kRight ⊆ kRight, kLeft ⊆ k, kRight ⊆ k, RUnion kLeft kRight k, RUnion vLeft vRight v, RDistinct vLeft vRight, Ord (FieldRec kLeft), Ord (FieldRec kRight), Ord (FieldRec k)) -- FIXME: This can be simplified somewhat.
    => FieldCube kLeft vLeft -> FieldCube kRight vRight -> FieldCube k v
(⋈) = C.join (C.Joiner rjoin rcast rcast) runion


(⋉) :: (Ord (FieldRec kRight), kRight ⊆ kLeft)
    => FieldCube kLeft vLeft -> FieldCube kRight vRight -> FieldCube kLeft vLeft
(⋉) = C.semijoin $ C.Joiner undefined undefined rcast


(▷) :: (Ord (FieldRec kRight), kRight ⊆ kLeft)
    => FieldCube kLeft vLeft -> FieldCube kRight vRight -> FieldCube kLeft vLeft
(▷) = C.antijoin $ C.Joiner undefined undefined rcast


(×) :: Set (FieldRec ks) -> Set (FieldRec ks') -> Set (FieldRec (ks ++ ks'))
(×) s1 s2 = S.fromDistinctAscList $ liftA2 (<+>) (S.toAscList s1) (S.toAscList s2)
