import asyncio
import discord
import mysql.connector
from redbot.core import commands, checks
from redbot.core.utils.chat_formatting import pagify
import json
import datetime
import importlib.machinery
loader = importlib.machinery.SourceFileLoader('dbconfig', 'C:/Users/dcsded/Scripts/RedBot/mycogs/40thBot/dbconfig.py')
dbconfig = loader.load_module('dbconfig')

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

    def determine_state(self, status):
        state = "Online"
        if abs(datetime.datetime.now() - status["updated"]) > datetime.timedelta(minutes=5):
            state = "Offline"  
            return state    
        if status["online"] == "False":
            state = "Offline"
        if status["isPaused"] == "True" and status["online"] == "True":
            state = "Paused"
        return state   

    def determine_color(self, status):
        if status == "Online":
            return 0x05e400
        if status == "Paused":
            return 0xFF9700
        return 0xFF0000

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
        # Get online status for selected instance (key)
        self.conn.execute("SELECT pe_OnlineStatus_instance,pe_OnlineStatus_theatre,pe_OnlineStatus_name,pe_OnlineStatus_pause,pe_OnlineStatus_multiplayer,pe_OnlineStatus_realtime,pe_OnlineStatus_modeltime,pe_OnlineStatus_players,pe_OnlineStatus_updated FROM pe_onlinestatus WHERE pe_OnlineStatus_instance = %s", (key,))
        result = list(self.conn.fetchone())
        #Add keys for returned results 
        onlineStatusColumn = ["server_instance", "theatre", "missionName", "isPaused", "online", "realtime", "modeltime", "players", "updated"]
        status = dict(zip(onlineStatusColumn, result))
        # DCS has default pilot connected. Remove from player count to get accurate number
        if status["players"] >= 1:
            status["players"] = status["players"] - 1
        # Add server name and alias to status just in case
        status.update({"serverName": self.dbconfig.servers[status["server_instance"]]["serverFullname"]})
        status.update({"alias": self.dbconfig.servers[status["server_instance"]]["alias"]})
        return status
    
    async def get_players(self, key):
        # Get all pilot name unique ID pairs except default DCS pilot
        self.conn.execute("SELECT pe_DataPlayers_ucid, pe_DataPlayers_lastname FROM pe_dataplayers WHERE pe_DataPlayers_ucid <> '40b7ff04fd4ddce40d53302d8db853c3'")
        pilotUID = dict(self.conn.fetchall())
        # Get pilot UIDs connected to each server instance and team except default DCS pilot
        self.conn.execute("SELECT pe_OnlinePlayers_side, pe_OnlinePlayers_ucid FROM pe_onlineplayers WHERE pe_OnlinePlayers_ucid <> '40b7ff04fd4ddce40d53302d8db853c3' AND pe_OnlinePlayers_instance = %s", (key,))
        pilotList = self.conn.fetchall()
        # Create dictionary to match returned coalition values
        number_to_side = {0:'Spectator', 1:'Red', 2:'Blue'}
        # coalition number indicates the list of players:
        coalition_player_name_list = {i : [] for i in number_to_side}
        # Look up each player's human-readable name and add to appropriate sub-list
        for coalition, uid in pilotList:
            human_name = pilotUID[uid]
            coalition_player_name_list[coalition].append(human_name)
        return coalition_player_name_list

    def get_mission_time(self, status):
        # Translate epoch time to human readable. Time the mission has been active. != server uptime
        time_seconds = datetime.timedelta(seconds=float(status["modeltime"]))
        return str(time_seconds).split(".")[0]


    async def embedMessage(self, status, playerList):
        # Instantiate health to determine state (online, paused, or offline) and set color (green, yellow, red)
        health = ServerHealth(status)
        embed = discord.Embed(color=health.color)
        embed.set_author(name=status["serverName"], icon_url="https://40thsoc.org/img/logo.png")
        # embed.set_thumbnail(url="https://40thsoc.org/img/logo.png")
        embed.add_field(name="Status", value=health.state, inline=True)
        embed.add_field(name="Map", value=status["theatre"], inline=True)
        embed.add_field(name="Mission", value=status["missionName"], inline=True)
        embed.add_field(name="Players", value=f'{status["players"]}/48', inline=True)
        if health.state == "Online":
            embed.add_field(name="Mission Time", value=self.get_mission_time(status), inline=True)
            embed.add_field(name="Updated", value=status["updated"], inline=True)
            # Add field and list players that are connected for their respective side
            for num, coalition in enumerate(playerList):
                if len(playerList[num]) > 0 and coalition == 0:
                    message = ''
                    for pilot in playerList[num]:
                        message += f"```{pilot}```\n"
                    embed.add_field(name="Spectators", value=message, inline=False)   
                if len(playerList[num]) > 0 and coalition == 1:
                    message = ''
                    for pilot in playerList[num]:
                        message += f"```css\n{pilot}\n```"
                    embed.add_field(name="Redfor", value=message, inline=True)
                if len(playerList[num]) > 0 and coalition == 2:
                    message = ''
                    for pilot in playerList[num]:
                        message += f"```ini\n{pilot}\n```"
                    embed.add_field(name="Blufor", value=message, inline=True)
        elif health.state == "Paused":
            embed.add_field(name="Mission Time", value=self.get_mission_time(status), inline=True)
            embed.add_field(name="Updated", value=status["updated"], inline=True)
        else:
            embed.add_field(name="Mission Time", value=self.get_mission_time(status), inline=True)
            embed.add_field(name=f"{health.state} since", value=status["updated"], inline=True)

            
        return embed


    @commands.command(name="serverlist")
    async def _servers(self, context):
        """Displays the list of tracked servers"""
        servers = self.dbconfig.servers
        if not servers:
            await context.send("No servers currently being tracked")
            return
        message = "\nTracking the following servers:\n"
        for key in servers:
            instance = servers[key]["instance"]
            alias = servers[key]["alias"]
            message += (f"{instance} - {alias}\n")
        message += "```fix\n Type \'?server <instance #>\' to get the status. Or \'?server all\' for status on all instances\n```"
        await context.send(message)


    @commands.command(name = "server")
    async def server_status(self, context, key):
        """Gets server status. Use ?serverlist to see tracked servers"""
        blocked = "Unable to message you the details of the server you requested. Either you blocked me or you disabled DMs from this server."
        async def respond_in_pm(text: str = None, embed = None) -> bool:
            if text is None and embed is None:
                raise ValueError("nothing to respond with")
            try:
                await context.message.author.send(text, embed=embed)
            except discord.http.Forbidden:
                # don't attempt to PM immediately after PMing fails due to being blocked
                if context.guild:
                    await context.send(blocked)
                    return False
            return True

        if context.guild:
            if not await respond_in_pm("Please only use `?server` in PMs with me."):
                # abort early, since the rest won't go through either
                return

        # Check if instance is tracked server or all keyword           
        if key != 'all':
            try:
                if int(key) not in self.dbconfig.servers: 
                    await respond_in_pm(f"{key} is not a tracked server instance")
                    return
            except ValueError:
                    await respond_in_pm(f"{key} is not a tracked server instance")
                    return
        
        try:
            # Send all server statuses if all keyword
            if key == 'all':
                servers = self.dbconfig.servers
                for key in servers:
                    status = await self.get_status(key)
                    playerList = await self.get_players(key)
                    message = await self.embedMessage(status, playerList) 
                    try:
                        await respond_in_pm(embed=message)
                    except discord.errors.HTTPException:
                        print("Server status failed to send message:", message, "from", status)
                        raise
            else:
                status = await self.get_status(key)
                playerList = await self.get_players(key)
                message = await self.embedMessage(status, playerList)
                try:
                    await respond_in_pm(embed=message)
                except discord.errors.HTTPException:
                    print("Server status failed to send message:", message, "from", status)
                    raise
            try:
                await context.message.add_reaction('✅')
            except discord.http.Forbidden:  # if blocked by a user, this would also fail
                pass
        except ErrorGettingStatus as e:
            await respond_in_pm("Status unknown right now.")
            print("Error getting status. Response code was " + str(e.status))
