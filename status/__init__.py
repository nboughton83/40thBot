from .server_status import DCSserverStatus

def setup(bot):
    bot.add_cog(DCSserverStatus(bot))