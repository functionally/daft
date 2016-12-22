{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies          #-}


module Data.Daft.DataCube (
-- * Types
  DataCube(..)
, Rekeyer(..)
, Gregator(..)
, Joiner(..)
) where


import Control.Arrow ((&&&))
import Data.Maybe (isJust, mapMaybe)
import GHC.Exts (Constraint)


class DataCube (cube :: (* -> Constraint) -> * -> * -> *) where

  cmap :: (v -> v') -> cube kc k v -> cube kc k v'

  cempty :: kc k => cube kc k v

  cappend :: kc k => cube kc k v -> cube kc k v -> cube kc k v

  evaluate :: kc k => cube kc k v -> k -> Maybe v

  evaluable :: kc k => cube kc k v -> k -> Bool
  evaluable = (isJust .) . evaluate

  select :: (v -> Bool) -> cube kc k v -> cube kc k v
  select = selectWithKey . const

  selectWithKey :: (k -> v -> Bool) -> cube kc k v -> cube kc k v

  selectKeys :: (k -> Bool) -> cube kc k v -> cube kc k v
  selectKeys = selectWithKey . (const .)

  selectRange :: Ord k => Maybe k -> Maybe k -> cube kc k v -> cube kc k v

  toTable :: kc k => (k -> v -> a) -> [k] -> cube kc k v -> [a]
  toTable combiner ks cube = mapMaybe (evaluate $ projectWithKey combiner cube) ks

  knownKeys :: cube kc k v -> [k]

  withKnown :: cube kc k v -> ([k] -> cube kc k v -> r) -> r
  withKnown cube f = f (knownKeys cube) cube

  knownSize :: cube kc k v -> Int
  knownSize = length . knownKeys

  knownEmpty :: cube kc k v -> Bool
  knownEmpty = null . knownKeys

  toKnownTable :: kc k => (k -> v -> a) -> cube kc k v -> [a]
  toKnownTable f = uncurry (toTable f) . (knownKeys &&& id)

  selectKnownMinimum :: Ord k => cube kc k v -> Maybe (k, v)

  selectKnownMaximum :: Ord k => cube kc k v -> Maybe (k, v)

  project :: (v -> v') -> cube kc k v -> cube kc k v'
  project = projectWithKey . const

  projectWithKey :: (k -> v -> v') -> cube kc k v -> cube kc k v'

  projectKnownKeys :: (k -> k') -> cube kc k v -> ks'

  rekey :: kc' k' => Rekeyer k k' -> cube kc k v -> cube kc' k' v

  aggregate :: kc' k' => Gregator k k' -> ([v] -> v') -> cube kc k v -> cube kc' k' v'
  aggregate = (. const) . aggregateWithKey

  aggregateWithKey :: kc' k' => Gregator k k' -> (k' -> [v] -> v') -> cube kc k v -> cube kc' k' v'

  disaggregate :: kc' k' => Gregator k' k -> (v -> v') -> cube kc k v -> cube kc' k' v'
  disaggregate = (. const) . disaggregateWithKey
  
  disaggregateWithKey :: kc' k' => Gregator k' k -> (k' -> v -> v') -> cube kc k v -> cube kc' k' v'

  joinSelf :: (kc k1, kc k2, kc k3) => Joiner k1 k2 k3 -> (v1 -> v2 -> v3) -> cube kc k1 v1 -> cube kc k2 v2 -> cube kc k3 v3


data Rekeyer a b =
  Rekeyer
  {
    rekeyer   :: a -> b
  , unrekeyer :: b -> a
  }


data Gregator a b =
  Gregator
  {
    aggregator    :: a -> b
  , disaggregator :: b -> [a]
  }


data Joiner kLeft kRight k =
  Joiner
  {
    joiner    :: kLeft -> kRight -> Maybe k
  , castLeft  :: k -> kLeft
  , castRight :: k -> kRight
  }
