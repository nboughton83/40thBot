from .server_status import DCSServerStatus

def setup(bot):
    bot.add_cog(DCSServerStatus(bot))