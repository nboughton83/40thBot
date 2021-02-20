import asyncio
import discord
import mysql.connector
from redbot.core import commands, checks
from redbot.core.utils.chat_formatting import pagify
import json
import datetime
import arrow
from . import dbconfig

class ErrorGettingStatus(Exception):
    def __init__(self, statusCode):
        self.status=statusCode

class ServerHealth:
    """
    Returns a ServerHealth with a health status string indicating "Online", "Paused", "Offline"
    and a `color` for use in graphics.
    Green - Online
    Orange - Paused
    Red - Offline
    """
    def __init__(self, status):
        self.state = self.determine_state(status)
        self.color = self.determine_color(self.state)
        #self.uptime = self.determine_uptime(self.status, self.uptime_data[server_key])

    def determine_state(self, status):
        state = "Online"
        if status["online"] == "False":
            state = "Offline"
        if status["isPaused"] == "True" and status["online"] == "True":
            state = "Paused"
        return state   

    def determine_color(self, status):
        if status == "Online":
            return 0x05e400
        if status == "Unhealthy":
            return 0xFF9700
        return 0xFF0000


"""
    def determine_uptime(self, status, uptime_data):
        now = arrow.utcnow()
        if "status" not in self.uptime_data:
            return self.determine_delta(now, uptime_data["time"])
        if self.uptime_data['status'] == status:
            current_uptime = self.determine_delta(now, uptime_data["time"])
            return current_uptime

    def store_uptime(self, status, time, server_key):
        self.uptime_data[server_key]["status"] = status
        self.uptime_data[server_key]["time"] = time.for_json()

    def determine_delta(self, current, change):
        delta = current - arrow.get(change)
        days = delta.days
        hours,remainder = divmod(delta.seconds,3600)
        minutes,seconds = divmod(remainder,60)
        return "{0} hours {1} minutes {2} seconds".format(
            hours, minutes, seconds
        ) 
"""

class DCSServerStatus(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        self.dbconfig = dbconfig
        self.killPoll = False
        self.last_key_checked = None
        self.presence_cycle_time_seconds = 5
        self.db = mysql.connector.connect(host=dbconfig.DB_HOST, user=dbconfig.DB_USERNAME, password=dbconfig.DB_PASSWORD, database=dbconfig.DB_DATABASE)
        self.conn = self.db.cursor()
        self.db.autocommit = True
        self.start_polling()


    def cog_unload(self):
        #kill the polling
        self.killPoll = True
        self.session.close()

    def start_polling(self):
        asyncio.ensure_future(self.poll())
        print("Server Status polling started")

    async def get_next_key(self):
        key = None
        servers = list(self.dbconfig.servers)
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
        server_data = self.dbconfig.servers[server_key]
        game = f"{status['players']} players in {server_data['alias']} playing {status['missionName']}"
        health = ServerHealth(status)
        bot_status=discord.Status.online
        if health.state == "Paused":
            bot_status=discord.Status.idle
            game = f"{server_data['alias']} server paused - {status['missionName']}"
        elif health.state == "Offline":
            bot_status=discord.Status.dnd
            game=f"{server_data['alias']} server offline"
        await self.bot.change_presence(status=bot_status, activity=discord.Game(name=game))

    async def get_status(self, key):
        self.conn.execute("SELECT pe_OnlineStatus_instance,pe_OnlineStatus_theatre,pe_OnlineStatus_name,pe_OnlineStatus_pause,pe_OnlineStatus_multiplayer,pe_OnlineStatus_realtime,pe_OnlineStatus_modeltime,pe_OnlineStatus_players,pe_OnlineStatus_updated FROM pe_onlinestatus WHERE pe_OnlineStatus_instance = %s", (key,))
        result = list(self.conn.fetchone())
        onlineStatusColumn = ["server_instance", "theatre", "missionName", "isPaused", "online", "realtime", "modeltime", "players", "updated"]
        status = dict(zip(onlineStatusColumn, result))
        if status["players"] >= 1:
            status["players"] = status["players"] - 1
        status.update({"serverName": self.dbconfig.servers[status["server_instance"]]["serverFullname"]})
        status.update({"alias": self.dbconfig.servers[status["server_instance"]]["alias"]})
        return status


    async def embedMessage(self, status, alias):
        health = ServerHealth(status)
        embed = discord.Embed(color=health.color)
        embed.set_author(name=status["serverName"], icon_url="https://40thsoc.org/img/logo.png")
        embed.set_thumbnail(url="https://40thsoc.org/img/logo.png")
        embed.add_field(name="Status", value=health.state, inline=True)
        embed.add_field(name="Mission", value=status["missionName"], inline=True)
        embed.add_field(name="Map", value=status["theatre"], inline=True)
        embed.add_field(name="Players", value="{}/{}".format(status["players"], status["maxPlayers"]), inline=True)
        embed.add_field(name="METAR", value=self.get_metar(status))
        if health.status == "Online":
            embed.add_field(name="Mission Time", value=self.get_mission_time(status), inline=True)
        else:
            embed.add_field(name="{} Since".format(health.status), value=health.uptime, inline=True)
        return embed
