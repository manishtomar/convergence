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

data NodeCondition = Enabled | NodeDraining | Disabled deriving (Eq, Show)

data CLBConfig = CLBConfig 
    { port :: Int, 
      weight :: Int, 
      condition :: NodeCondition
    } deriving (Eq, Show)

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

data ServerState = Active | Error | Build | Draining deriving Eq

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

type DesiredGroupState = (LaunchConfig,         -- launch config used to create servers
                          Int,                  -- desired number of servers
                          DesiredCLBConfigs,    -- CLB configurations
                          NominalDiffTime,      -- draining timeout
                          NominalDiffTime)      -- build timeout

type RealGroupState = ([NovaServer],    -- current list of servers
                       [CLBNode],       -- current list of all CLB's all Nodes
                       UTCTime)         -- current time

-- | This corresponds to steps defined in
-- https://github.com/rackerlabs/otter/blob/master/otter/convergence/steps.py
data Step
    = CreateServer LaunchConfig
    | DeleteServer ServerID
    | SetMetadataOnServer ServerID String String
    | AddNodeToCLB CLBID IPAddress CLBConfig
    | RemoveNodeFromCLB CLBID NodeID
    | ChangeCLBNode CLBID NodeID
                    Int             -- weight
                    NodeCondition   -- condition
    deriving (Eq, Show)


-- | converge implementation

-- por combines 2 predicates with or operator
por :: (a -> Bool) -> (a -> Bool) -> a -> Bool
por p1 p2 = \x -> or [p1 x, p2 x]

-- is the server building for long time?
buildTooLong timeout now server = diffUTCTime now (created server) > timeout

-- is server in given state
isState st server = state server == st

-- filter nodes of a server based on its address
serverNodes server = filter (\n -> address n == servicenetAddress server)

-- drain server and nodes and finally delete server too if all the nodes are removed
drainAndDelete :: NovaServer -> [CLBNode] -> NominalDiffTime -> UTCTime -> [Step]
drainAndDelete server nodes timeout now =
    let steps = concatMap (\n -> drain server n timeout now) nodes
        removes = [RemoveNodeFromCLB (lbId node) (nodeId node) | node <- nodes]
    in steps ++ if steps == removes then [DeleteServer (getId server)] else []

-- drain server and corresponding node if required otherwise delete the node
drain :: NovaServer -> CLBNode -> NominalDiffTime -> UTCTime -> [Step]
drain server node timeout now =
    case condition (config node) of
        Disabled -> delete
        NodeDraining -> case connections node of
                            Just conn -> if conn == 0 then delete else deleteIfTimedout
                            Nothing -> deleteIfTimedout
        Enabled -> [ChangeCLBNode (lbId node) (nodeId node) (weight $ config node) NodeDraining]
                   ++ if state server /= Draining then [sm] else []
    where delete = [RemoveNodeFromCLB (lbId node) (nodeId node)]
          deleteIfTimedout = if diffUTCTime now (drainedAt node) > timeout
                             then delete else []
          sm = SetMetadataOnServer (getId server) "rax:auto_scaling_draining" "draining"

-- | returns steps to move given servers to desired CLB configs
clbSteps :: DesiredCLBConfigs -> [NovaServer] -> [CLBNode] -> [Step]
clbSteps lbs servers nodes = concatMap serverSteps servers
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
    let unwanted = isState Error `por` buildTooLong buildTimeout now
        draining = isState Draining
        validServers = filter (not . (unwanted `por` draining)) servers
        valid = length validServers
        (remove, inGroup) = splitAt (valid - desired) (sortBy (comparing created) validServers)
    in [CreateServer lc | _ <- [0..(desired - valid)]] ++ 
       [DeleteServer (getId s) | s <- filter unwanted servers] ++
       [RemoveNodeFromCLB (lbId node) (nodeId node) 
            | s <- filter unwanted servers, node <- serverNodes s nodes] ++
       concatMap drainDeleteServer (remove ++ filter draining servers) ++
       clbSteps lbs (filter (isState Active) inGroup) nodes
    where drainDeleteServer = \s -> drainAndDelete s (serverNodes s nodes) drainingTimeout now
