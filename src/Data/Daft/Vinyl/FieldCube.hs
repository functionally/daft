{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}


module Data.Daft.Vinyl.FieldCube (
-- * Types
  type (↝)(..)
, type (+↝)
, type (-↝)
, FieldGregator
-- * Conversion
, fromRecords
, toRecords
, toKnownRecords
, τ
-- * Evaluation
, (!)
-- * Selection, projection, and aggregation
, σ
, π
, κ
, δ
, ω
, ρ
-- * Joins
, (⋈)
, (⋉)
, (▷)
, (×)
) where


import Control.Applicative (liftA2)
import Data.Daft.DataCube (DataCube, FunctionCube, Join, Joinable, TableCube)
import Data.Daft.TypeLevel (Intersection)
import Data.Daft.Vinyl.TypeLevel (RDistinct, RJoin(rjoin), RUnion(runion))
import Data.Maybe (fromMaybe)
import Data.Set (Set)
import Data.Vinyl.Core ((<+>))
import Data.Vinyl.Derived (FieldRec)
import Data.Vinyl.Lens (type (⊆), rcast)
import Data.Vinyl.TypeLevel (type (++))

import qualified Data.Daft.DataCube as C -- (Gregator(..), Joiner(Joiner), aggregateWithKey, antijoin, disaggregateWithKey, evaluate, fromTable, join, knownKeys, projectWithKey, reify, selectWithKey, semijoin, toKnownTable, toTable)
import qualified Data.Set as S (fromDistinctAscList, map, toAscList)


data ks ↝ vs = forall cube . DataCube cube => FieldCube (cube (FieldRec ks) (FieldRec vs))


type ks +↝ vs = TableCube (FieldRec ks) (FieldRec vs)


type ks -↝ vs = FunctionCube (FieldRec ks) (FieldRec vs)


fromRecords :: (ks ⊆ as, vs ⊆ as, RUnion ks vs as, Ord (FieldRec ks)) => [FieldRec as] -> ks ↝ vs
fromRecords = FieldCube . C.fromTable rcast rcast


toRecords :: (Ord (FieldRec ks), RUnion ks vs as) => [FieldRec ks] -> ks +↝ vs -> [FieldRec as]
toRecords = C.toTable runion


toKnownRecords :: (Ord (FieldRec ks), RUnion ks vs as) => ks +↝ vs -> [FieldRec as]
toKnownRecords = C.toKnownTable runion


τ :: (b ⊆ a) => FieldRec a -> FieldRec b
τ = rcast


(!) :: (Ord (FieldRec ks)) => ks ↝ vs -> FieldRec ks -> FieldRec vs
FieldCube cube ! key =
  fromMaybe (error "FieldCube: key not found.")
    $ C.evaluate cube key


σ :: (FieldRec ks -> FieldRec vs -> Bool) -> ks ↝ vs -> ks ↝ vs
σ selector (FieldCube cube) = FieldCube $ C.selectWithKey selector cube


π :: (FieldRec ks -> FieldRec vs -> FieldRec ws) -> ks ↝ vs -> ks ↝ ws
π projector (FieldCube cube) = FieldCube $ C.projectWithKey projector cube


ρ :: Ord (FieldRec ks) => Set (FieldRec ks) -> ks ↝ vs -> ks +↝ vs
ρ support (FieldCube cube) = C.reify support cube


type FieldGregator as bs = C.Gregator (FieldRec as) (FieldRec bs)


κ :: (ks0 ⊆ ks, ks ⊆ (ks' ++ ks0), ks' ⊆ ks, Ord (FieldRec ks), Ord (FieldRec ks')) => Set (FieldRec ks0) -> (FieldRec ks' -> [FieldRec vs] -> FieldRec vs') -> ks ↝ vs -> ks' ↝ vs' -- FIXME: Instead of subset, use sum.
κ keys reducer (FieldCube cube) =
  FieldCube 
    $ C.aggregateWithKey
    C.Gregator
    {
      C.aggregator    = rcast
    , C.disaggregator = flip map (S.toAscList keys) . (rcast .) . (<+>)
    }
    reducer
    cube


δ :: (ks0 ⊆ ks', ks' ⊆ (ks ++ ks0), ks ⊆ ks', Ord (FieldRec ks), Ord (FieldRec ks')) => Set (FieldRec ks0) -> (FieldRec ks' -> FieldRec vs -> FieldRec vs') -> ks ↝ vs -> ks' ↝ vs' -- FIXME: Instead of subset, use sum.
δ keys expander (FieldCube cube) =
  FieldCube
    $ C.disaggregateWithKey
    C.Gregator
    {
      C.aggregator    = rcast
    , C.disaggregator = flip map (S.toAscList keys) . (rcast .) . (<+>)
    }
    expander
    cube


ω :: (ks' ⊆ ks, Ord (FieldRec ks')) => ks ↝ vs -> Set (FieldRec ks')
ω (FieldCube cube) = S.map rcast $ C.knownKeys cube


(⋈) :: (Eq (FieldRec (Intersection kLeft kRight)), Intersection kLeft kRight ⊆ kLeft, Intersection kLeft kRight ⊆ kRight, kLeft ⊆ k, kRight ⊆ k, RUnion kLeft kRight k, RUnion vLeft vRight v, RDistinct vLeft vRight, Ord (FieldRec kLeft), Ord (FieldRec kRight), Ord (FieldRec k)) -- FIXME: This can be simplified somewhat.
    => kLeft ↝ vLeft -> kRight ↝ vRight -> k ↝ v
(FieldCube cubeLeft) ⋈ (FieldCube cubeRight) = FieldCube $ join0 cubeLeft cubeRight
infixl 6 ⋈


join0 :: (Eq (FieldRec (Intersection kLeft kRight)), Intersection kLeft kRight ⊆ kLeft, Intersection kLeft kRight ⊆ kRight, kLeft ⊆ k, kRight ⊆ k, RUnion kLeft kRight k, RUnion vLeft vRight v, RDistinct vLeft vRight, Ord (FieldRec kLeft), Ord (FieldRec kRight), Ord (FieldRec k), DataCube cubeLeft, DataCube cubeRight, DataCube cube, Joinable cubeLeft cubeRight cube) -- FIXME: This can be simplified somewhat.
    => cubeLeft (FieldRec kLeft) (FieldRec vLeft) -> cubeRight (FieldRec kRight) (FieldRec vRight) -> cube (FieldRec k) (FieldRec v)
join0 = C.join (C.Joiner rjoin rcast rcast) runion


(⋉) :: (Ord (FieldRec kRight), kRight ⊆ kLeft)
    => kLeft ↝ vLeft -> kRight ↝ vRight -> kLeft ↝ vLeft
(FieldCube cubeLeft) ⋉ (FieldCube cubeRight) = FieldCube $ C.semijoin (C.Joiner undefined undefined rcast) cubeLeft cubeRight
infixl 6 ⋉


(▷) :: (Ord (FieldRec kRight), kRight ⊆ kLeft)
    => kLeft ↝ vLeft -> kRight ↝ vRight -> kLeft ↝ vLeft
(FieldCube cubeLeft) ▷ (FieldCube cubeRight) = FieldCube $ C.antijoin (C.Joiner undefined undefined rcast) cubeLeft cubeRight
infixl 6 ▷


(×) :: Set (FieldRec ks) -> Set (FieldRec ks') -> Set (FieldRec (ks ++ ks'))
(×) s1 s2 = S.fromDistinctAscList $ liftA2 (<+>) (S.toAscList s1) (S.toAscList s2)
infixl 8 ×
