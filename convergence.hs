import Data.Time.Clock (UTCTime, NominalDiffTime, diffUTCTime)
import Data.List (sortBy, find)
import Data.Ord (comparing)
import Data.Maybe (fromJust)
import qualified Data.HashMap.Lazy as HM


type UUID = String

-- | The CLB and Nova model corresponds to model defined in
-- https://github.com/rackerlabs/otter/blob/master/otter/convergence/model.py

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

-- | This corresponds to steps defined in
-- https://github.com/rackerlabs/otter/blob/master/otter/convergence/steps.py
data Step
    = CreateServer LaunchConfig
    | DeleteServer ServerID
    | SetMetadataOnServer ServerID String String
    | AddNodeToCLB CLBID IPAddress CLBConfig
    | RemoveNodeFromCLB CLBID NodeID
    | ChangeCLBNode CLBID NodeID
                    Int -- weight
                    Int -- condition


-- | converge implementation

-- | 'any_preds' takes list of predicates and return predicate that returns
-- true if any of the predicates return true
any_preds :: [a -> Bool] -> a -> Bool
any_preds ps = \x -> or $ map (\p -> p x) ps

-- is the server building for long time?
buildTooLong timeout now server = diffUTCTime now (created server) > timeout

-- is server in ERROR state
isError server = state server == Error

-- filter nodes of a server based on its address
serverNodes server = filter (\n -> address n == servicenetAddress server)

-- drain and delete server by putting it in draining first if needed, otherwise
-- delete server and corresponding node
drainAndDelete :: NovaServer -> CLBNode -> NominalDiffTime -> UTCTime -> [Step]
drainAndDelete server node draining now = []

-- | returns steps to move given servers to desired CLB configs
clbSteps :: DesiredCLBConfigs -> [NovaServer] -> [CLBNode] -> [Step]
clbSteps lbs servers nodes = concat $ map serverSteps servers
    where serverSteps s = serverClbSteps lbs (serverNodes s nodes) (servicenetAddress s)

-- | returns steps to move given IPAddress (of a server) to desired CLBs
serverClbSteps :: DesiredCLBConfigs -> [CLBNode] -> IPAddress -> [Step]
serverClbSteps lbConfigs nodes ip =
    let desired = HM.fromList [((cid, port conf), conf) | (cid, confs) <- lbConfigs, conf <- confs]
        actual = HM.fromList [((lbId node, port $ config node), node) | node <- nodes]
    in [AddNodeToCLB cid ip conf
            | ((cid, _), conf) <- HM.toList $ HM.difference desired actual] ++
       [RemoveNodeFromCLB (lbId node) (nodeId node)
            | node <- HM.elems $ HM.difference actual desired] ++
       [ChangeCLBNode cid (nodeId node) (weight conf) (condition conf)
            | ((cid, port), node) <- HM.toList $ HM.intersection actual desired,
              let conf = fromJust $ HM.lookup (cid, port) desired, conf /= config node]

-- converge function in
-- https://github.com/rackerlabs/otter/blob/master/otter/convergence/planning.py
converge :: DesiredGroupState -> RealGroupState -> [Step]
converge (lc, desired, lbs, drainingTimeout, buildTimeout) (servers, nodes, now) = 
    -- TODO: Use Set instead
    let unwanted = filter (any_preds [isError, buildTooLong buildTimeout now]) servers
        valid = length servers - length unwanted
        active = filter (`notElem` unwanted) servers
        remove = take (valid - desired) (sortBy (comparing created) active)
    in [CreateServer lc | _ <- [0..(desired - valid)]] ++ 
       [DeleteServer (getId s) | s <- unwanted] ++
       [RemoveNodeFromCLB (lbId node) (nodeId node) 
            | s <- unwanted, node <- serverNodes s nodes] ++
       concat ([drainAndDelete s node drainingTimeout now
                    | s <- remove, node <- serverNodes s nodes]) ++
       clbSteps lbs (filter (`notElem` unwanted ++ remove) servers) nodes
