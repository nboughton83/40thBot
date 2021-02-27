from .dcs_tracking import DCSTrackingTools

def setup(bot):
    bot.add_cog(DCSTrackingTools(bot))