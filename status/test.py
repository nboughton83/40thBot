import asyncio
import mysql.connector
import json
import aiohttp
import datetime
import hashlib
import os
from bs4 import BeautifulSoup
import dbconfig


class ServerHealth:

    def __init__(self, server_instance, data):
        self.uptime_data = data
        self.status = self.determine_status(updateTime, self.uptime_data[server_key], server_key)
        self.color = self.determine_color(self.status)
        self.uptime = self.determine_uptime(self.status, self.uptime_data[server_key])

    def determine_status(self, updateTime, uptime_data, server_key):
        now = arrow.utcnow()
        status = "Online"
        if "status" not in uptime_data:
            uptime_data["status"] = ""
        if (updateTime < now.shift(seconds=-60)):
            status = "Unhealthy"
        if (updateTime < now.shift(seconds=-100)):
            status = "Offline"
        if status != uptime_data["status"]:
            self.store_uptime(status, updateTime, server_key)
        return status

    def determine_color(self, status):
        if status == "Online":
            return "Green"
        if status == "Unhealthy":
            return "Yellow"
        return "Red"

class DCS():

    def __init__(self):
        self.killPoll = False
        self.last_key_checked = None
        self.presence_cycle_time_seconds = 5
        self.db = mysql.connector.connect(host=dbconfig.DB_HOST, user=dbconfig.DB_USERNAME, password=dbconfig.DB_PASSWORD, database=dbconfig.DB_DATABASE)
        self.conn = self.db.cursor()
        self.db.autocommit = True

    def start_polling(self):
        asyncio.ensure_future(self.poll())
        print("Server Status polling started")

    async def get_next_key(self):
        key = None
        servers = list(dbconfig.servers)
        key = self.last_key_checked
        try:
            key = servers[(servers.index(key) + 1) % len(servers)]
        except ValueError:
            key = servers[0]
        self.last_key_checked = key
        return key

    async def poll(self):
        try:
            key = await self.get_next_key()
            if not key:
                return #still runs finally
            status = await self.get_status(key)
            await self.set_presence(status, key)
        except Exception as e:
            print("Server Status poll encountered an error. skipping this poll: ", str(e))
        finally:
            if self.killPoll:
                print("Server Status poll killswitch received. Not scheduling another poll")
                return
            await asyncio.sleep(self.presence_cycle_time_seconds)
            asyncio.ensure_future(self.poll())

    async def set_presence(self, status, server_key):
        server_data = await self.dbconfig.servers(server_key)
        #game = f"{status["players"]} players in {server_data["alias"]} playing {status["missionName"]}"
        game = "Test"
        health = await self.determine_health(status, server_key)
        bot_status=discord.Status.online
        if health.status == "Paused":
            bot_status=discord.Status.idle
            game="Paused - " + game
        elif health.status == "Offline":
            bot_status=discord.Status.dnd
            game=f"{server_data["alias"]} server offline"
        await self.bot.change_presence(status=bot_status, activity=discord.Game(name=game))


    async def get_status(self, key):
        self.conn.execute("SELECT pe_OnlineStatus_instance,pe_OnlineStatus_theatre,pe_OnlineStatus_name,pe_OnlineStatus_pause,pe_OnlineStatus_multiplayer,pe_OnlineStatus_modeltime,pe_OnlineStatus_players,pe_OnlineStatus_updated FROM pe_onlinestatus WHERE pe_OnlineStatus_instance = %s", (key,))
        result = list(await self.conn.fetchone())
        onlineStatusColumn = ["server_instance", "theatre", "missionName", "isPaused", "online", "modeltime", "players", "updated"]
        status = dict(zip(onlineStatusColumn, result))
        if status["players"] >= 1:
            status["players"] = status["players"] - 1
        status.update({"serverName": dbconfig.servers[status["server_instance"]]["serverFullname"]})
        status.update({"alias": dbconfig.servers[status["server_instance"]]["serverAlias"]})
        return status

    def determine_health(self, status):
        last_update = arrow.get(status["updateTime"])
        return ServerHealth(last_update, server_key, self.config.get_raw("servers"))







