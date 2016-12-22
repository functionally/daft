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


class DataCube (kc :: * -> Constraint) (cube :: * -> * -> *) where

  cmap :: (v -> v') -> cube k v -> cube k v'

  cempty :: kc k => cube k v

  cappend :: kc k => cube k v -> cube k v -> cube k v

  evaluate :: kc k => cube k v -> k -> Maybe v

  evaluable :: kc k => cube k v -> k -> Bool
  evaluable = (isJust .) . evaluate

  select :: (v -> Bool) -> cube k v -> cube k v
  select = selectWithKey . const

  selectWithKey :: (k -> v -> Bool) -> cube k v -> cube k v

  selectKeys :: (k -> Bool) -> cube k v -> cube k v
  selectKeys = selectWithKey . (const .)

  selectRange :: Ord k => Maybe k -> Maybe k -> cube k v -> cube k v

  toTable :: kc k => (k -> v -> a) -> [k] -> cube k v -> [a]
  toTable combiner ks cube = mapMaybe (evaluate $ projectWithKey combiner cube) ks

  knownKeys :: cube k v -> [k]

  withKnown :: cube k v -> ([k] -> cube k v -> r) -> r
  withKnown cube f = f (knownKeys cube) cube

  knownSize :: cube k v -> Int
  knownSize = length . knownKeys

  knownEmpty :: cube k v -> Bool
  knownEmpty = null . knownKeys

  toKnownTable :: kc k => (k -> v -> a) -> cube k v -> [a]
  toKnownTable f = uncurry (toTable f) . (knownKeys &&& id)

  selectKnownMinimum :: Ord k => cube k v -> Maybe (k, v)

  selectKnownMaximum :: Ord k => cube k v -> Maybe (k, v)

  project :: (v -> v') -> cube k v -> cube k v'
  project = projectWithKey . const

  projectWithKey :: (k -> v -> v') -> cube k v -> cube k v'

  projectKnownKeys :: (k -> k') -> cube k v -> ks'

  rekey :: kc k' => Rekeyer k k' -> cube k v -> cube' k' v

  aggregate :: kc k' => Gregator k k' -> ([v] -> v') -> cube k v -> cube' k' v'
  aggregate = (. const) . aggregateWithKey

  aggregateWithKey :: kc k' => Gregator k k' -> (k' -> [v] -> v') -> cube k v -> cube' k' v'

  disaggregate :: kc k' => Gregator k' k -> (v -> v') -> cube k v -> cube' k' v'
  disaggregate = (. const) . disaggregateWithKey
  
  disaggregateWithKey :: kc k' => Gregator k' k -> (k' -> v -> v') -> cube k v -> cube' k' v'

  joinSelf :: (kc k1, kc k2, kc k3) => Joiner k1 k2 k3 -> (v1 -> v2 -> v3) -> cube k1 v1 -> cube k2 v2 -> cube k3 v3


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
