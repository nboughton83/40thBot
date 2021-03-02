import asyncio
import discord
import mysql.connector
from redbot.core import commands, checks
from redbot.core.data_manager import bundled_data_path
from redbot.core.utils.chat_formatting import pagify
import json
import datetime
import re
import pygsheets
from pygsheets.datarange import DataRange
import pandas as pd
import importlib.machinery
loader = importlib.machinery.SourceFileLoader('dbconfig', 'C:/Users/dcsded/Scripts/RedBot/mycogs/40thBot/dbconfig.py')
dbconfig = loader.load_module('dbconfig')

class ErrorGettingStatus(Exception):
    def __init__(self, statusCode):
        self.status=statusCode

class DCSTrackingTools(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.pd = pd
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
        return final_list

    def get_attendance_list(self, mission_id):
        # fetch list of users and associated ID
        self.conn.execute("SELECT pe_DataPlayers_id,pe_DataPlayers_lastname FROM pe_dataplayers WHERE pe_DataPlayers_id <> 42")
        player_dictionary = dict(self.conn.fetchall())
        # fetch list of aireframes and associated ID
        self.conn.execute("SELECT pe_DataTypes_id,pe_DataTypes_name FROM pe_datatypes")
        airframe_dictionary = dict(self.conn.fetchall())
        # fetch mission name by mission ID
        self.conn.execute("SELECT pe_DataMissionHashes_hash from pe_datamissionhashes WHERE pe_DataMissionHashes_id = %s", (mission_id,)) 
        mission = self.conn.fetchone()[0]
        # fetch players who attended mission by mission ID
        self.conn.execute("SELECT pe_LogStats_playerid,pe_LogStats_typeid FROM pe_logstats WHERE pe_LogStats_masterslot <> -1 AND pe_LogStats_missionhash_id = %s ORDER BY ps_time", (mission_id,)) 
        participant_list = dict(self.conn.fetchall())
        
        # correct names to match attendance google sheet
        attendance_dictionary = {}
        corrected_names = {'A-10C_2': 'A-10C', 'AV8BNA': 'AV-8B', 'F-14A-135-GR': 'F-14A', 'F-14B': 'F-14B Pilot', 'F-14B_2': 'F-14B RIO', 'F-16C_50': 'F-16C', 'FA-18C_hornet': 'F/A-18C', 'Ka-50': 'KA-50', 'M-2000C': 'M2000C', 'UH-1H_2': 'UH-1H', 'UH-1H_3': 'UH-1H', 'UH-1H_4': 'UH-1H'}
        corrected_airframe = {k: corrected_names.get(v, v) for k, v in airframe_dictionary.items()}

        # Remove instance number and Perun version from mission name string
        mission = mission.split("@")
        # Drop hour,min,sec from mission date
        mission_date = mission[3].split("_")[0]
        mission_date = datetime.datetime.strptime(mission_date, '%Y%m%d').strftime('%m/%d/%Y')
        mission_name = mission[0]
        
        # remove 40th tags from username
        regex = re.compile(r'(?:\[40th\s*SOC\])*\s*(.+)$', re.IGNORECASE)
        for playerID, playerName in player_dictionary.items():
            taglessName = regex.findall(playerName)
            player_dictionary[playerID] = taglessName[0]

        # compile final dictionary by taking pilot ID and airframe ID and replacing with real names
        playerDict = {player_dictionary.get(k, k):v for k, v in participant_list.items()}
        pilot_dict = {k: corrected_airframe.get(v, v) for k, v in playerDict.items()}

        # create correct keys for pandas dataframe columns
        attendance_dictionary["Date"] = mission_date
        attendance_dictionary["Mission Name"] = mission_name
        attendance_dictionary["Participant"] = list(pilot_dict.keys())
        attendance_dictionary["Airframe"] = list(pilot_dict.values())

        return attendance_dictionary


    async def upload_attendance(self, attendance_list):
        # build dataframe to upload to google sheet from attendance list
        df = self.pd.DataFrame(attendance_list, columns = ['Date', 'Mission Name', 'Participant', 'Airframe'])

        # setup authorization for google 
        gc = pygsheets.authorize(service_account_file=bundled_data_path(self) / "service_account.json")
        # open specific google sheet based on key, stored in dbconfig file
        sh = gc.open_by_key(self.dbconfig.attendance_sheet_key)
        # set worksheet to second tab
        wks = sh[1]
        # pull data on current sheet for use in determing where to place dataframe and coloring
        cells = wks.get_col(1, include_tailing_empty=False, returnas='matrix')
        last_row = len(cells)
        data_rows = len(df) + last_row
        beige = (0.9882353, 0.8980392, 0.8039216, 0)
        no_color = (None, None, None, None)
        white = (1, 1, 1, 0)

        # Set pandas dataframme as cell values
        wks.set_dataframe(df, start=(last_row + 1,1), copy_head=False, extend=True)
        # add cell background coloring for each different mission dataframe
        previous_color = wks.cell(f'A{last_row - 1}').color
        if previous_color == no_color or previous_color == white and last_row != 1:
            model_cell = pygsheets.Cell("A2")
            model_cell.color = beige
            DataRange(f'A{last_row + 1}',f'E{data_rows}', worksheet=wks).apply_format(model_cell, fields = "userEnteredFormat.backgroundColor")
        elif previous_color == beige and last_row != 1:
            model_cell = pygsheets.Cell("A2")
            model_cell.color = white
            DataRange(f'A{last_row + 1}',f'E{data_rows}', worksheet=wks).apply_format(model_cell, fields = "userEnteredFormat.backgroundColor")  


    async def embedMessage(self, attendance_list):
        # Build discord embed message for pilot list
        participant = attendance_list["Participant"]
        airframe = attendance_list["Airframe"]
        full_list = {participant[i]: airframe[i] for i in range(len(participant))}
        embed_list = '\n'.join(f"{line[0]}, {line[1]}" for line in full_list.items())
        # Add fields for discord message
        embed = discord.Embed()
        embed.set_author(name=attendance_list['Mission Name'], icon_url="https://40thsoc.org/img/logo.png")
        embed.add_field(name="Date:", value=attendance_list["Date"], inline=False)
        embed.add_field(name="Participants:", value=f"```fix\n{embed_list}\n```", inline=False)
        return embed


    @commands.command(name = "mlist")
    async def _missions(self, context):
        """Displays a list of recent missions and ID"""
        # get list of last 5 missions
        mission_list = self.get_missions()
        # create discord embed message
        mlist = '\n'.join(f"{line[0]}, '{line[1]}'" for line in mission_list)
        embed = discord.Embed(color=0xFF0000)
        embed.add_field(name="Mission List", value=mlist, inline=True)
        embed.set_footer(text="Use ?attendance <mission id> to upload and post attendance list")
        await context.send(embed=embed)

    @commands.command(name = "attendance")
    async def __attendance(self, context, mission_id):
        """Get attendance for mission and post-upload"""
        blocked = "Unable to message attendance list. Either you blocked me or you disabled DMs from this server."
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

        # check if mission ID is valid
        mission_list = self.get_missions()
        inlist = [item for item in mission_list if item[0] == int(mission_id)]
        if not inlist:
            await respond_in_pm(f"{mission_id} is not a valid ID")
            return

        try:
            attendance_list = self.get_attendance_list(mission_id)
            message = await self.embedMessage(attendance_list)
            try:
                await context.send(embed=message)
                await self.upload_attendance(attendance_list)
            except discord.errors.HTTPException:
                print("Attendance failed to send message:", message)
                raise
        except ErrorGettingStatus as e:
            await respond_in_pm("Error getting attendance right now.")
            print("Error getting attendance. Response code was " + str(e))
