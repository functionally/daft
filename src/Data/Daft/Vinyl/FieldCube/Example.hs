{-# LANGUAGE DataKinds                       #-}

{-# OPTIONS_GHC -fno-warn-unused-binds       #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}


module Data.Daft.Vinyl.FieldCube.Example (
  main
) where


import Data.Daft.DataCube (fromFunction)
import Data.Daft.Vinyl.FieldCube (FieldCube, (⋈), (!), fromRecords, toRecords)
import Data.Daft.Vinyl.FieldRec ((<:), readFieldRecs, showFieldRecs)
import Data.List (intercalate)
import Data.Vinyl.Core ((<+>))
import Data.Vinyl.Derived (FieldRec, SField(..), (=:))


-- Types for field names.
type StateUSPS = '("State USPS"     , String)
type StateName = '("State Name"     , String)
type CityName  = '("City Name"      , String)
type StateHash = '("State Hash"     , Int   )
type Longitude = '("Longitude [deg]", Double)
type Latitude  = '("Latitude [deg]" , Double)


-- Functions for accessing fields.
sStateUSPS = SField :: SField StateUSPS
sStateName = SField :: SField StateName
sCityName  = SField :: SField CityName
sStateHash = SField :: SField StateHash
sLongitude = SField :: SField Longitude
sLatitude  = SField :: SField Latitude


-- Some data about states.
states :: FieldCube '[StateUSPS] '[StateName]
states =
  let
    statesRaw =
      [
        ["State USPS", "State Name"     ]
      , ["\"CA\""    , "\"California\"" ]
      , ["\"CT\""    , "\"Connecticut\""]
      , ["\"NM\""    , "\"New Mexico\"" ]
      , ["\"CO\""    , "\"Colorado\""   ]
      ]
    Right stateRecs = readFieldRecs statesRaw :: Either String [FieldRec '[StateName, StateUSPS]]
  in
    fromRecords stateRecs


-- A hash function on state names.
hashStates :: FieldCube '[StateUSPS] '[StateHash]
hashStates =
  fromFunction $ \k ->
    let
      stateUSPS = sStateUSPS <: k
    in
       return . (sStateHash =:) . product $ map fromEnum stateUSPS


-- Some data about cities.
cities :: FieldCube '[StateUSPS, CityName] '[Longitude, Latitude]
cities =
  let
    citiesRaw =
      [
        ["State USPS", "City Name"        , "Longitude [deg]", "Latitude [deg]"]
      , ["\"CA\""    , "\"Los Angeles\""  , "-118.2437"      , "34.0522"       ]
      , ["\"CA\""    , "\"San Francisco\"", "-122.4194"      , "37.7749"       ]
      , ["\"NM\""    , "\"Santa Fe\""     , "-105.9378"      , "35.6870"       ]
      , ["\"CO\""    , "\"Golden\""       , "-105.2211"      , "39.7555"       ]
      , ["\"CO\""    , "\"Denver\""       , "-104.9903"      , "39.7392"       ]
      ]
    Right cityRecs = readFieldRecs citiesRaw :: Either String [FieldRec '[CityName, Latitude, Longitude, StateUSPS]]
  in
    fromRecords cityRecs


-- Some areas of interest.
interest :: [FieldRec '[StateUSPS, CityName]]
interest =
  [
    sStateUSPS =: "CA" <+> sCityName =: "Los Angeles"
  , sStateUSPS =: "CA" <+> sCityName =: "San Francisco"
  , sStateUSPS =: "CT" <+> sCityName =: "New Haven"
  , sStateUSPS =: "NM" <+> sCityName =: "Santa Fe"
  ]


-- Simple example of some joins of tables and functions.
main :: IO ()
main =
  do
    let
      x :: FieldCube '[StateUSPS] '[StateName, StateHash]
      x = states ⋈ hashStates
    putStrLn ""
    putStrLn "Example of evaluation:"
    print $ x ! (sStateUSPS =: "CA")
    let
      y :: FieldCube '[StateUSPS, CityName] '[Longitude, Latitude, StateName, StateHash]
      y = cities ⋈ x
    putStrLn ""
    putStrLn "Result of some joins with tables and functions:"
    putStrLn . unlines . fmap (intercalate "\t")
      $ showFieldRecs (toRecords interest y :: [FieldRec '[StateUSPS, StateName, StateHash, CityName, Longitude, Latitude]])