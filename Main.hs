
{-# LANGUAGE TemplateHaskell, QuasiQuotes #-}

module Main where

import           Control.Applicative
import           Control.Monad (forever, void)
import           Control.Concurrent (threadDelay)
import           Control.Exception (SomeException, catch)
import qualified Data.Vector as V
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as L

import qualified Data.Configurator as Config
import qualified Database.PostgreSQL.Simple as PG
import           Database.PostgreSQL.Simple.FromRow
import           Database.PostgreSQL.Simple.SqlQQ (sql)
import           Network.Mail.Mime
import           Data.Pool (Pool, createPool, withResource)
import           System.Posix.Syslog

import qualified System.Environment as Env


type Addr = Text
type Mime = Text

data Email = Email
  { ident :: Int
  , from  :: Addr
  , to    :: [Addr]
  , cc    :: [Addr]
  , reply :: Maybe Addr
  , mime  :: Mime
  , subj  :: Text
  , body  :: L.ByteString
  } deriving Show

instance FromRow Email where
  fromRow = Email
    <$> field <*> field
    <*> (V.toList <$> field) <*> (V.toList <$> field)
    <*> field <*> field
    <*> field <*> field


main :: IO ()
main = do
  prog <- Env.getProgName
  Env.getArgs >>= \case
    [configPath] -> do
      -- FIXME: logUpTo from config
      withSyslog prog [PID] USER (logUpTo Debug) $ do
        syslog Info $ "Loading config from " ++ configPath
        conf <- Config.load [Config.Required configPath]

        Just host <- Config.lookup conf "pg.host"
        Just port <- Config.lookup conf "pg.port"
        Just user <- Config.lookup conf "pg.user"
        Just pwd  <- Config.lookup conf "pg.pass"
        Just db   <- Config.lookup conf "pg.db"
        Just tout <- Config.lookup conf "queue.scan_interval"
        Just copy <- Config.lookup conf "smtp.copy"

        syslog Info $ "Connecting to Postgres on " ++ host
        let cInfo = PG.ConnectInfo host port user pwd db
        pgPool <- createPool (PG.connect cInfo) PG.close
            1 -- number of distinct sub-pools
              -- time for which an unused resource is kept open
            (fromIntegral $ tout*2)
            5 -- maximum number of resources to keep open

        syslog Info "Starting main loop"
        loop tout copy pgPool

    _ -> error $ "Usage: " ++ prog ++ " <config.conf>"



loop :: Int -> Text -> Pool PG.Connection -> IO ()
loop tout copy pgPool = forever $ do
  let go = getJob pgPool >>= \case
        [] -> return ()
        [msg] -> do
          syslog Info $ "Got job: " ++ show (ident msg)

          let msg' = msg {cc = copy : cc msg}
          res <- sendMail msg' `catch` \e ->
                  return (Left $ show (e :: SomeException))

          case res of
            Right {} -> endJob pgPool (ident msg) "done"
            Left err -> do
              syslog Error err
              endJob pgPool (ident msg) "error"
        res -> error $ "BUG: " ++ show res
  go `catch` \e -> syslog Error $ show (e :: SomeException)
  threadDelay $ tout * 10^(6::Int)


getJob
  :: Pool PG.Connection
  -> IO [Email]
getJob pgPool
  = withResource pgPool $ \c -> PG.query_ c
    [sql|
      update "Email"
        set status = 'processing',
            mtime  = statement_timestamp()
        where id in
          (select id from "Email"
            where status = 'please-send'
            limit 1)
        returning id, "from", "to", cc, reply, mime, subject, body
    |]


endJob :: Pool PG.Connection -> Int -> Text -> IO ()
endJob pgPool ident st
  = void $ withResource pgPool $ \c -> PG.execute c
    [sql|
      update "Email"
        set status = ?, mtime = statement_timestamp()
        where id = ?
    |]
    (st, ident)


sendMail :: Email -> IO (Either String ())
sendMail (Email{..}) = do
  let mkAddr = Address Nothing . T.strip
  renderSendMailCustom "/usr/sbin/sendmail" ["-t", "-i", "-f", T.unpack from]
    $ (emptyMail $ mkAddr from)
      { mailTo      = map mkAddr to
      , mailCc      = map mkAddr cc
      , mailHeaders = [("Subject", subj)]
                    ++ maybe [] (\addr -> [("Reply-To", addr)]) reply
      , mailParts   = [[Part mime QuotedPrintableText Nothing [] body]]
      }
  return $ Right ()
