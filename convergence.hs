import Data.Time.Clock (UTCTime, NominalDiffTime, diffUTCTime)
import Data.List (sortBy, find)
import Data.Ord (comparing)
import Data.Maybe (isJust, fromJust)
import Data.HashMap.Lazy (fromList, toList, difference)


type UUID = String

-- CLB model

type IPAddress = String
type CLBID = String
type NodeID = String

data CLBConfig = CLBConfig 
    { port :: Int, 
      weight :: Int, 
      condition :: Int
    } deriving (Eq, Show)

data NodeCondition = Enabled | NodeDraining | Disabled
data NodeType = Primary | Secondary
data CLBNode = CLBNode 
    { lbId :: CLBID,
      nodeId :: NodeID,
      address :: IPAddress,
      drainedAt :: UTCTime,
      connections :: Maybe Int,
      config :: CLBConfig
    }

-- Nova model

type LaunchConfig = String  -- for now
type ServerID = UUID

data ServerState = Active | Error | Build | ServerDraining deriving Eq

data NovaServer = NovaServer 
    { getId :: ServerID,
      state :: ServerState,
      created :: UTCTime,
      servicenetAddress :: IPAddress
    }

instance Eq NovaServer where
    (==) s1 s2 = getId s1 == getId s2

-- Desired and Steps

type DesiredCLBConfigs = [(CLBID, [CLBConfig])]

type DesiredGroupState = (LaunchConfig, Int, DesiredCLBConfigs, NominalDiffTime, NominalDiffTime)

type RealGroupState = ([NovaServer], [CLBNode], UTCTime)

data Step
    = CreateServer LaunchConfig
    | DeleteServer ServerID
    | SetMetadataOnServer ServerID String String
    | AddNodeToCLB CLBID IPAddress CLBConfig
    | RemoveNodeFromCLB CLBID NodeID
    | ChangeCLBNode CLBID NodeID Int Int


-- | converge implementation

-- | 'any_preds' takes list of predicates and return predicate that returns
-- true if any of the predicates return true
any_preds :: [a -> Bool] -> a -> Bool
any_preds ps = \x -> or $ map (\p -> p x) ps

buildTooLong timeout now server = diffUTCTime now (created server) > timeout

isError server = state server == Error

nodeByAddress :: [CLBNode] -> NovaServer -> Maybe CLBNode
nodeByAddress nodes server = find (\n -> address n == servicenetAddress server) nodes

clbSteps :: DesiredCLBConfigs -> [NovaServer] -> [CLBNode] -> NominalDiffTime -> [Step]
clbSteps lbs servers nodes timeout = []

-- | returns steps to move given IPAddress (of a server) to desired CLBs
serverClbSteps :: DesiredCLBConfigs -> [CLBNode] -> NominalDiffTime -> IPAddress -> [Step]
serverClbSteps lbConfigs nodes draining ip = 
    let desired = fromList [((cid, port conf), conf) | (cid, confs) <- lbConfigs, conf <- confs]
        actual = fromList [((lbId node, port $ config node), node) | node <- nodes]
    in [AddNodeToCLB cid ip conf | ((cid, _), conf) <- toList $ difference desired actual] ++
       [RemoveNodeFromCLB cid (nodeId node) | ((cid, _), node) <- toList $ difference actual desired]

converge :: DesiredGroupState -> RealGroupState -> [Step]
converge (lc, desired, lbs, drainingTimeout, buildTimeout) (servers, nodes, now) = 
    -- TODO: Use Set instead
    let unwanted = filter (any_preds [isError, buildTooLong buildTimeout now]) servers
        valid = length servers - length unwanted
        active = filter (`notElem` unwanted) servers
        deletingServers = unwanted ++ take (valid - desired) (sortBy (comparing created) active)
    in [CreateServer lc | _ <- [0..(desired - valid)]] ++ 
       [DeleteServer (getId s) | s <- deletingServers] ++
       [RemoveNodeFromCLB (lbId node) (nodeId node) 
            | s <- deletingServers, let maybeNode = nodeByAddress nodes s, 
              isJust maybeNode, let node = fromJust maybeNode] ++
       clbSteps lbs (filter (`notElem` deletingServers) servers) nodes drainingTimeout
