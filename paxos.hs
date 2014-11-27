--------------------
-- CS 271 Project --
-- Lawton Nichols --
--------------------

{- 

    ssh -i lawton-oregon.pem ubuntu@54.148.215.60
    ssh -i lawton-ireland.pem ubuntu@54.76.113.250
    ssh -i lawton-frankfurt.pem ubuntu@54.93.180.143
    ssh -i lawton-singapore.pem ubuntu@54.169.171.162
    ssh -i lawton-tokyo.pem ubuntu@54.65.136.5

    ./paxos [54,148,215,60]
    ./paxos [54,76,113,250]
    ./paxos [54,93,180,143]
    ./paxos [54,169,171,162]
    ./paxos [54,65,136,5]

    git clone https://github.com/lawtonnichols/CS-271-Project.git

-}

-- Some socket-handling code taken from https://www.haskell.org/haskellwiki/Implement_a_chat_server

import System.IO
import System.Exit
import System.Environment
import Network.Socket
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Control.Exception
import Data.Word
import Data.IORef
import qualified Data.Map.Strict as Map

majority :: Int
majority = floor (numberOfSites / 2) + 1
           where numberOfSites = fromIntegral $ length sites

type IPAddress = [Int] 

sites = [[54,148,215,60], [54,76,113,250], [54,93,180,143], [54,169,171,162], [54,65,136,5]] :: [IPAddress]

type BallotNumber = Int

type LogIndex = Int

data Ballot = Ballot BallotNumber IPAddress deriving (Show, Read, Eq)

-- create an ordering on Ballots so we can compare them inside of Paxos
instance Ord Ballot where
    (Ballot num ip) > (Ballot num2 ip2) = (num > num2) || (num == num2 && ip > ip2)
    (<=) b1 b2 = not (b1 > b2)

data NetworkMessage = Prepare LogIndex Ballot
                    | Ack LogIndex Ballot Ballot CLICommand
                    | Accept LogIndex Ballot CLICommand
                    | Decide LogIndex CLICommand
                    | SendMeYourLog 
                    | MyLogIs [CLICommand] 
                    | TryToAdd CLICommand 
                    | Blank deriving (Show, Read, Eq)

data CLICommand = Deposit Double
                | Withdraw Double
                | Balance
                | Fail
                | Unfail 
                | Quit 
                | Bottom 
                | ViewLog
                | Recover deriving (Show, Read, Eq, Ord)

-- taken from http://stackoverflow.com/questions/10459988/how-do-i-catch-read-exceptions-in-haskell
maybeRead :: Read a => String -> Maybe a
maybeRead s = case reads s of
    [(x, "")] -> Just x
    [(x, rest)] | isWhiteSpace rest -> Just x
    _         -> Nothing

isWhiteSpace "" = True
isWhiteSpace (x:xs) = (x == ' ' || x == '\t' || x == '\n')
                    && (isWhiteSpace xs)

debug = False
putStrLnDebug x = if debug then putStrLn x else return ()

runCLICommand :: CLICommand -> IORef [CLICommand] -> IO ()
runCLICommand command myLog = do
    -- if this is a withdrawal, make sure we can do this
    currentLog <- readIORef myLog
    let balance = getBalance currentLog
    let isItOkayToContinue = case command of 
                               (Withdraw amt) -> if balance - amt >= 0.0 then True else False
                               _              -> True
    let isTheAmountPositive = case command of 
                               (Deposit amt) -> if amt > 0.0 then True else False
                               (Withdraw amt) -> if amt > 0.0 then True else False
                               _              -> True
    if not isTheAmountPositive then 
        putStrLn "Error: You can't deposit or withdraw an amount <= $0.00."
    else if not isItOkayToContinue then
        putStrLn "Error: You can't withdraw more than the current balance."
    else do
        addrInfo <- getAddrInfo (Just (defaultHints { addrFamily=AF_INET })) (Just "localhost") (Just $ "4242")
        let sockAddr = addrAddress (head addrInfo)
        sendingSock <- socket AF_INET Stream 0
        putStrLnDebug "runCLICommand: connecting to localhost"
        connect sendingSock sockAddr
        send sendingSock (show (TryToAdd command))
        sClose sendingSock
    --putStrLn $ "The command you entered was: " ++ (show command)

getAddress host port = do 
    addrInfo <- getAddrInfo (Just (defaultHints { addrFamily=AF_INET })) (Just host) (Just $ (show port))
    let sockAddr = addrAddress (head addrInfo)
    return sockAddr

getBalance :: [CLICommand] -> Double
getBalance (l:ls) = case l of 
                      Deposit amt  -> amt + (getBalance ls)
                      Withdraw amt -> (-amt) + (getBalance ls)
                      _            -> getBalance ls
getBalance [] = 0.0

repl :: IORef Bool -> IORef [CLICommand] -> IO ()
repl isFailSet myLog = do
    putStr "$> "
    hFlush stdout
    line <- getLine
    let command = maybeRead line :: Maybe CLICommand
    case command of
        Just Quit -> exitSuccess
        Just Fail -> atomicModifyIORef' isFailSet (\_ -> (True, ()))
        Just Unfail -> atomicModifyIORef' isFailSet (\_ -> (False, ()))
        Just Balance -> do {l <- readIORef myLog; putStrLn $ "Balance: " ++ (show $ getBalance l); hFlush stdout;}
        Just ViewLog -> do {l <- readIORef myLog; putStrLn $ "Current Log: " ++ (show l); hFlush stdout;}
        Just Recover -> readLogInto myLog
        Just c -> runCLICommand c myLog
        _ -> putStrLn "Parse error; please check your input."
    repl isFailSet myLog

serve sock ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals isFailSet = do
    conn <- accept sock
    shouldIContinue <- liftM not $ readIORef isFailSet
    if shouldIContinue then do
        forkIO (processConnection conn ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals)
        return ()
    else close (fst conn)
    serve sock ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals isFailSet

processConnection (sock, SockAddrInet port host) ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals = do
    -- get the remote message
    hdl <- socketToHandle sock ReadWriteMode
    hSetBuffering hdl NoBuffering
    line <- catch (hGetLine hdl) (\e -> let exc = (e :: SomeException) in return "")
    let message = if length line == 0 then Just Blank else maybeRead line :: Maybe NetworkMessage
    putStrLnDebug $ "processConnection: got message " ++ (show message)
    hClose hdl

    -- only continue if we didn't get a blank message
    if message /= Just Blank then do
        -- send replies back to the same host, but only on its server port
        let newAddr = SockAddrInet 4242 host
        newSock <- socket AF_INET Stream 0
        putStrLnDebug $ "processConnection: connecting to " ++ (show host)
        connect newSock newAddr
        hdl2 <- socketToHandle newSock ReadWriteMode
        hSetBuffering hdl2 NoBuffering

        -- process the message if it's valid
        case message of
            Just Blank -> return ()
            Just m -> processMessage hdl2 m ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals
            _ -> do putStr $ "\n*** Invalid message received: " ++ line ++ " ***\n$> "
                    hFlush stdout
                    --hPutStrLn hdl2 "Error"
        hClose hdl2
    else return ()

increment :: Ballot -> Ballot
increment (Ballot num processID) = (Ballot (num+1) processID)

-- TODO: make this actually work
myAddress :: IO IPAddress
myAddress = do
    args <- getArgs
    --putStrLn $ "getting args: " ++ (show args)
    let ip = read (head args) :: IPAddress
    --putStrLn $ "returning " ++ (show ip)
    return ip

sendToEveryoneButMe :: NetworkMessage -> IO ()
sendToEveryoneButMe message = do
    myAddr <- myAddress
    let everyoneButMe = filter (/= myAddr) sites
    --x <- sequence $ map (sendMessage message) everyoneButMe
    x <- sequence $ map (\site -> forkIO (sendMessage message site)) everyoneButMe
    return ()

sendToMe :: NetworkMessage -> IO ()
sendToMe message = do
    myAddr <- myAddress
    x <- forkIO (sendMessage message myAddr)
    return ()

sendToEveryone :: NetworkMessage -> IO ()
sendToEveryone message = do
    --x <- sequence $ map (sendMessage message) sites
    x <- sequence $ map (\site -> forkIO (sendMessage message site)) sites
    return ()

toIPString :: IPAddress -> String
toIPString host = init $ foldr (\x acc -> (show x) ++ "." ++ acc) "" host

sendMessage :: NetworkMessage -> IPAddress -> IO ()
sendMessage m host = do
    putStrLnDebug $ "sending " ++ (show m) ++ " to " ++ (show host)
    addrInfo <- getAddrInfo (Just (defaultHints { addrFamily=AF_INET })) (Just $ toIPString host) (Just $ "4242")
    let sockAddr = addrAddress (head addrInfo)
    sendingSock <- socket AF_INET Stream 0
    
    catch (do connect sendingSock sockAddr
              send sendingSock (show m);
              sClose sendingSock) 
        (\e -> let exc = (e :: SomeException) in return ())
    
    

saveLog :: IORef [CLICommand] -> IO ()
saveLog myLog = do
    l <- readIORef myLog
    writeFile "myLog.log" (show l)

readLogInto :: IORef [CLICommand] -> IO ()
readLogInto myLog = do
    l <- readFile "myLog.log"
    let newMyLog = read l :: [CLICommand]
    atomicModifyIORef' myLog (\old -> (newMyLog,()))

{- 

    Possible way to handle multiple deposits
    ========================================

    When you receive an Ack, instead of making sure all the values
    are all Bottom, just make sure they're all either Deposit or 
    Bottom. Change myVal to be a list of values, and send 
    ("accept", BallotNum, [old deposit (if any), new deposit]). Now, 
    when the values are accepted you can decide on both deposits at 
    once, and their order in the log is unimportant.

-}

processMessage :: Handle -> NetworkMessage -> IORef Ballot -> IORef Ballot -> IORef CLICommand ->  IORef Int -> IORef (Map.Map (Ballot, CLICommand) Int) -> IORef [CLICommand] -> IORef CLICommand -> IORef [(CLICommand, Ballot)] -> IO ()
processMessage hdl message ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals = do
    putStrLnDebug $ "*** received " ++ (show message) ++ " ***"
    hFlush stdout
    -- TODO: figure out where to reset the values & counters
    case message of 
        TryToAdd command -> do
            -- send prepare to everyone else & update myVal
            -- set my current value to this value
            atomicModifyIORef' myVal (\old -> (command, ()))
            -- increment my ballotNum
            newBallotNum <- atomicModifyIORef' ballotNum (\old -> (increment old, increment old))
            -- which log index do we want to update?
            currentLogLength <- liftM length $ readIORef myLog
            --sendToEveryoneButMe (Prepare currentLogLength newBallotNum)
            sendToEveryone (Prepare currentLogLength newBallotNum)
        Prepare logIndex bal -> do
            currentLogLength <- liftM length $ readIORef myLog
            if currentLogLength < logIndex then do
                -- we don't know enough; get everyone else's log & try again
                sendToEveryoneButMe SendMeYourLog
                threadDelay 200000 -- wait for .2 s
                -- try again
                processMessage hdl message ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals
            else if currentLogLength > logIndex then do
                -- they don't know enough; send over our log
                currentLog <- readIORef myLog
                hPutStrLn hdl $ show (MyLogIs currentLog)
            else do 
                -- send ack if ballot >= ballotNum
                currentBallotNum <- readIORef ballotNum
                currentAcceptVal <- readIORef acceptVal
                currentAcceptNum <- readIORef acceptNum
                if bal >= currentBallotNum then do
                    -- replace current ballotNum with bal
                    atomicModifyIORef' ballotNum (\old -> (bal, ()))
                    -- send ack
                    hPutStrLn hdl $ show (Ack logIndex bal currentAcceptNum currentAcceptVal)
                else return ()
        Ack logIndex ballot foreignAcceptNum foreignAcceptVal -> do
            currentLogLength <- liftM length $ readIORef myLog
            -- only accept this message if the log index is correct
            if logIndex == currentLogLength then do
                -- increment accept counter 
                newAckCount <- atomicModifyIORef' ackCounter (\old -> (old+1, old+1))
                -- add this received value to the list of received values
                newReceivedVals <- atomicModifyIORef' receivedVals (\old -> (old++[(foreignAcceptVal, foreignAcceptNum)], old++[(foreignAcceptVal, foreignAcceptNum)]))
                
                --putStrLn $ "newReceivedVals: " ++ (show newReceivedVals)
                --putStrLn $ "newAckCount: " ++ (show newAckCount)
                --putStrLn $ "majority: " ++ (show majority)

                -- do this only once, so when it's exactly equal to a majority
                -- TODO: Move the printing of SUCCESS/FAILURE to Accept
                if newAckCount == majority then do
                    if all (\(val,bal) -> val == Bottom) newReceivedVals then do
                        -- SUCCESS
                        putStr "SUCCESS\n$> "
                        hFlush stdout
                    else do
                        -- FAILURE
                        putStr "FAILURE\n$> "
                        hFlush stdout
                        -- change myVal
                        -- get val with max ballotNumber
                        let (maxVal, maxBal) = foldl (\(maxVal, maxBal) (val, bal) -> if bal > maxBal then (val, bal) else (maxVal, maxBal)) (Bottom, Ballot 0 [0,0,0,0]) newReceivedVals 
                        atomicModifyIORef' myVal (\old -> (maxVal, ()))
                    
                    -- no matter what, we need to send accept to everyone in this case
                    currentMyVal <- readIORef myVal
                    --sendToEveryoneButMe (Accept logIndex ballot currentMyVal)
                    sendToEveryone (Accept logIndex ballot currentMyVal)
                else return ()
            else return ()
        Accept logIndex b cliCommand -> do
            currentLogLength <- liftM length $ readIORef myLog
            if currentLogLength < logIndex then do
                -- we don't know enough; get everyone else's log & try again
                sendToEveryoneButMe SendMeYourLog
                threadDelay 200000 -- wait for .2 s
                -- try again
                processMessage hdl message ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals
            else if currentLogLength > logIndex then return ()
            else do
                currentBallotNum <- readIORef ballotNum
                -- increment this accept count
                newCount <- atomicModifyIORef' acceptCounter (\old -> let x = Map.lookup (b, cliCommand) old
                                                                        in case x of 
                                                                          Nothing -> (Map.insert (b, cliCommand)     1 old, 1)
                                                                          Just n  -> (Map.insert (b, cliCommand) (n+1) old, n+1))
                currentAcceptCounter <- readIORef acceptCounter

                if b >= currentBallotNum then do
                    -- change my ballotNum so that we don't keep sending out accepts
                    atomicModifyIORef' ballotNum (\(Ballot myN myIP) -> let (Ballot n ip) = b in ((Ballot (n+1) myIP), ()))

                    oldAcceptNum <- readIORef acceptNum 
                    newAcceptNum <- atomicModifyIORef' acceptNum (\old -> (b, b))
                    atomicModifyIORef' acceptVal (\old -> (cliCommand, ()))
                    -- send (Accept b cliCommand) to everyone (only the first time)
                    if newAcceptNum > oldAcceptNum then 
                        --sendToEveryoneButMe (Accept logIndex b cliCommand)
                        sendToEveryone (Accept logIndex b cliCommand)
                    else return ()
                else return ()

                -- decide if we've now received a majority
                if newCount == majority then do
                    sendToEveryoneButMe (Decide logIndex cliCommand)
                    sendToMe (Decide logIndex cliCommand)
                else return ()
        Decide logIndex cliCommand -> do
            -- we just decided on a value--update the next log entry
            -- make sure it's the right one
            currentLogLength <- liftM length $ readIORef myLog
            if currentLogLength > logIndex then
                return () -- do nothing; we know more
            else if currentLogLength < logIndex then do
                -- we don't know enough; get everyone else's log & try again
                sendToEveryoneButMe SendMeYourLog
                threadDelay 200000 -- wait for .2 s
                -- try again
                processMessage hdl message ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals
            else do
                -- we agree on the next index to update; update it
                atomicModifyIORef' myLog (\oldLog -> 
                    (oldLog ++ [cliCommand], ()))
                saveLog myLog
                -- reset everything
                atomicModifyIORef' ballotNum (\(Ballot oldN ip) -> ((Ballot 0 ip),()))
                atomicModifyIORef' acceptNum (\_ -> ((Ballot 0 [0,0,0,0]),()))
                atomicModifyIORef' acceptVal (\old -> (Bottom, ()))
                atomicModifyIORef' ackCounter (\old -> (0, ()))
                -- TODO: do I need to update the acceptCounter?
        SendMeYourLog -> do
            -- get the contents of my log and send it
            l <- readIORef myLog
            hPutStrLn hdl $ show (MyLogIs $ l)
        MyLogIs l -> do 
            currentLog <- readIORef myLog
            if length currentLog < length l then do
                -- reset everything (only once) 
                -- TODO: is this necessary?
                atomicModifyIORef' ballotNum (\(Ballot oldN ip) -> ((Ballot 0 ip),()))
                atomicModifyIORef' acceptNum (\_ -> ((Ballot 0 [0,0,0,0]),()))
                atomicModifyIORef' acceptVal (\old -> (Bottom, ()))
                atomicModifyIORef' ackCounter (\old -> (0, ()))
                putStrLn "Error: Log size was too small; any pending command has failed."
            else return ()

            -- update the local log if it's not as long as the received log
            atomicModifyIORef' myLog (\oldLog -> 
                if length oldLog < length l
                    then (l, ()) 
                else (oldLog, ()))
            saveLog myLog

main :: IO ()
main = do
    -- get my address (from command line argument)
    myAddr <- myAddress

    -- initialize shared variables
    ballotNum <- newIORef (Ballot 0 myAddr)
    acceptNum <- newIORef (Ballot 0 [0,0,0,0])
    acceptVal <- newIORef Bottom
    myLog     <- newIORef [] :: IO (IORef [CLICommand])
    myVal     <- newIORef Bottom
    receivedVals   <- newIORef [] :: IO (IORef [(CLICommand, Ballot)])
    ackCounter     <- newIORef 0 :: IO (IORef Int)
    acceptCounter  <- newIORef Map.empty :: IO (IORef (Map.Map (Ballot, CLICommand) Int))
    isFailSet      <- newIORef False

    -- set up the TCP server
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bindSocket sock (SockAddrInet 4242 iNADDR_ANY)
    listen sock 16
    forkIO (serve sock ballotNum acceptNum acceptVal ackCounter acceptCounter myLog myVal receivedVals isFailSet)

    -- start the REPL
    putStrLn "Enter \"Deposit XX.XX\", \"Withdraw XX.XX\", \"Balance\", \"Fail\", \"Unfail\", \"ViewLog\", \"Recover\", or \"Quit\""
    repl isFailSet myLog
