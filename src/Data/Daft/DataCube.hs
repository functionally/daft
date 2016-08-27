{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}


module Data.Daft.DataCube (
  DataCube
, fromTable
, fromFunction
, toTable
, knownKeys
, evaluate
, evaluable
, select
, selectWithKey
, selectKeys
, project
, projectWithKey
, projectKeys
, Gregator(..)
, fromKnownKeys
, aggregate
, aggregateWithKey
, Joiner(..)
, join
, semijoin
, antijoin
) where


import Control.Applicative ((<|>), liftA2)
import Control.Arrow ((&&&), (***))
import Control.Monad (guard)
import Data.Map (Map)
import Data.Maybe (catMaybes, isJust, mapMaybe)
import GHC.Exts (IsList(Item))

import qualified Data.Map as M (assocs, empty, filterWithKey, fromList, fromListWith, keys, lookup, mapWithKey, member, mergeWithKey, union)
import qualified GHC.Exts as L (IsList(..))


data DataCube k v =
    TableCube
    {
      table :: Map k v
    }
  | FunctionCube
    {
      function :: k -> Maybe v
    }

instance Functor (DataCube k) where
  fmap f TableCube{..}    = TableCube    $ fmap f table
  fmap f FunctionCube{..} = FunctionCube $ fmap f . function

instance Ord k => Applicative (DataCube k) where
  pure = FunctionCube . const . return
  TableCube table1 <*> TableCube table2 =
    TableCube
      $ M.mergeWithKey (const (Just .)) (const M.empty) (const M.empty) table1 table2
  cube1 <*> cube2 =
    FunctionCube $ \k ->
      do
        f <- cube1 `evaluate` k
        v <- cube2 `evaluate` k
        return $ f v

instance Ord k => Monoid (DataCube k v) where
  mempty = TableCube mempty
  mappend (TableCube table1) (TableCube table2) = TableCube $ M.union table1 table2
  mappend cube1 cube2 = FunctionCube $ \k -> evaluate cube1 k <|> evaluate cube2 k


fromTable :: (IsList as, a ~ Item as, Ord k) => (a -> k) -> (a -> v) -> as -> DataCube k v
fromTable keyer valuer = TableCube . M.fromList . fmap (keyer &&& valuer) . L.toList


fromFunction :: (k -> Maybe v) -> DataCube k v
fromFunction = FunctionCube


toTable :: (IsList ks, k ~ Item ks, IsList as, a ~ Item as, Ord k) => (k -> v -> a) -> ks -> DataCube k v -> as
toTable combiner ks cube = L.fromList $ mapMaybe (evaluate $ projectWithKey combiner cube) $ L.toList ks


knownKeys :: IsList ks => DataCube (Item ks) v -> ks
knownKeys = projectKeys id


evaluate :: Ord k => DataCube k v -> k -> Maybe v
evaluate TableCube{..}    = flip M.lookup table
evaluate FunctionCube{..} = function


evaluable :: Ord k => DataCube k v -> k -> Bool
evaluable TableCube{..}    = flip M.member table
evaluable FunctionCube{..} = isJust . function


select :: (v -> Bool) -> DataCube k v -> DataCube k v
select = selectWithKey . const


selectWithKey :: (k -> v -> Bool) -> DataCube k v -> DataCube k v
selectWithKey selector TableCube{..} =
  TableCube
    $ M.filterWithKey selector table
selectWithKey selector FunctionCube{..} =
  FunctionCube $ \k ->
    do
      v <- function k
      guard
        $ selector k v
      return v

selectKeys :: (k -> Bool) -> DataCube k v -> DataCube k v
selectKeys = selectWithKey . (const .)


project :: (v -> w) -> DataCube k v -> DataCube k w
project = projectWithKey . const


projectWithKey :: (k -> v -> w) -> DataCube k v -> DataCube k w
projectWithKey projector TableCube{..} = TableCube $ M.mapWithKey projector table
projectWithKey projector FunctionCube{..} = FunctionCube $ liftA2 fmap projector function


projectKeys :: IsList ks => (k -> Item ks) -> DataCube k v -> ks
projectKeys projector TableCube{..}    = L.fromList . fmap projector $ M.keys table
projectKeys _         FunctionCube{..} = L.fromList []


data Gregator a b =
  Gregator
  {
    aggregator    :: a -> b
  , disaggregator :: b -> [a]
  }


fromKnownKeys :: Eq k' => (k -> k') -> DataCube k v -> Gregator k k'
fromKnownKeys aggregator cube =
  let
    disaggregator k' = filter ((k' ==) . aggregator) $ knownKeys cube
  in
    Gregator{..}


aggregate :: Ord k' => Gregator k k' -> ([v] -> v') -> DataCube k v -> DataCube k' v'
aggregate = (. const) . aggregateWithKey


aggregateWithKey :: Ord k' => Gregator k k' -> (k' -> [v] -> v') -> DataCube k v -> DataCube k' v'
aggregateWithKey Gregator{..} reducer TableCube{..} =
  TableCube
    . M.mapWithKey reducer
    . M.fromListWith (++)
    . fmap (aggregator *** return)
    $ M.assocs table
aggregateWithKey Gregator{..} reducer FunctionCube{..} =
  FunctionCube $ \k' ->
    return
      . reducer k'
      . mapMaybe function
      $ disaggregator k'


data Joiner kLeft kRight k =
  Joiner
  {
    joiner    :: kLeft -> kRight -> Maybe k
  , castLeft  :: k -> kLeft
  , castRight :: k -> kRight
  }


join :: (Ord k1, Ord k2, Ord k3) => Joiner k1 k2 k3 -> (v1 -> v2 -> v3) -> DataCube k1 v1 -> DataCube k2 v2 -> DataCube k3 v3
join Joiner{..} combiner (TableCube table1) (TableCube table2) =
  TableCube
    . M.fromList
    $ catMaybes
    [
      (, v1 `combiner` v2) <$> k1 `joiner` k2
    |
      (k1, v1) <- M.assocs table1
    , (k2, v2) <- M.assocs table2
    ]
join Joiner{..} combiner cube1 cube2 =
  FunctionCube $ \k3 ->
    do
      v1 <- cube1 `evaluate` castLeft k3
      v2 <- cube2 `evaluate` castRight k3
      return $ v1 `combiner` v2


semijoin :: Ord k2 => Joiner k1 k2 k1 -> DataCube k1 v1 -> DataCube k2 v2 -> DataCube k1 v1
semijoin Joiner{..} = flip (selectKeys . (. castRight) . evaluable)


antijoin :: Ord k2 => Joiner k1 k2 k1 -> DataCube k1 v1 -> DataCube k2 v2 -> DataCube k1 v1
antijoin Joiner{..}= flip (selectKeys . (. castRight) . (not .) . evaluable)