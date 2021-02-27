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

class DCSTrackingTools(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.dbconfig = dbconfig
        self.db = mysql.connector.connect(host=dbconfig.DB_HOST, user=dbconfig.DB_USERNAME, password=dbconfig.DB_PASSWORD, database=dbconfig.DB_DATABASE)
        self.conn = self.db.cursor()
        self.db.autocommit = True

    def get_missions(self):
        # Get list of recent mission ID and names with 15 players or greater
        # Minimum 15 players because each player has at least 2 entries in logstats, observer "-1" and airframe "id" hence (HAVING COUNT (*) > 30)
        self.conn.execute("SELECT pe_LogStats_missionhash_id FROM pe_logstats GROUP BY pe_LogStats_missionhash_id HAVING COUNT(*) > 30 ORDER BY pe_LogStats_missionhash_id DESC LIMIT 5")
        recent_missions = self.conn.fetchall()
        # Prepare empty list for append
        final_list = []
        # For missions matching 15 players or greater, return Mission ID and Mission Name in list
        for mission in recent_missions:
            self.conn.execute("SELECT pe_DataMissionHashes_id,pe_DataMissionHashes_hash from pe_datamissionhashes WHERE pe_DataMissionHashes_id = %s", (mission[0],))
            mission_id_name = self.conn.fetchall()
            mission_name = mission_id_name[0][1]
            # Remove instance number and Perun version from mission name string
            mission_name = mission_name.split("@")
            # Select mission name and date from split list
            mission_name = f"{mission_name[0]} - {mission_name[3]}"
            shortened_mission_list = mission_id_name[0][0], mission_name
            final_list.append(shortened_mission_list)
        # Setup mission list file for output to Discord
        mission_list = '\n'.join(f"{line[0]}, '{line[1]}'" for line in final_list)
        return mission_list










    @commands.command(name="mlist")
    async def _missions(self, context):
        """Displays a list of recent missions and ID"""
        mlist = self.get_missions()
        embed = discord.Embed(color=0xFF0000)
        embed.add_field(name="Mission List", value=mlist, inline=True)
        await context.send(embed=embed)

