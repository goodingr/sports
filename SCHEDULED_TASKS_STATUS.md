# Scheduled Tasks Status

## ✅ Tasks Successfully Created!

### Task 1: NBA Forward Test - Predict
- **Status**: ✅ Created and Ready
- **Schedule**: Daily at 11:00 PM UTC (6:00 PM EST)
- **Next Run**: Today at 11:00 PM
- **Action**: Makes predictions on live NBA games before they start
- **Logs**: `logs\forward_test_predict_YYYYMMDD.log`

### Task 2: NBA Forward Test - Update
- **Status**: ✅ Created and Ready
- **Schedule**: Daily at 7:00 AM UTC (2:00 AM EST)
- **Next Run**: Tomorrow at 7:00 AM
- **Action**: Updates predictions with actual game results
- **Logs**: `logs\forward_test_update_YYYYMMDD.log`

## 🎯 What Happens Now

### Automatic Daily Workflow

**Every day at 6:00 PM EST:**
1. Task runs automatically
2. Fetches current NBA games with odds
3. Makes predictions using your trained model
4. Shows recommendations (bets with edge >= 0.06)
5. Saves predictions to `data/forward_test/predictions_master.parquet`
6. Logs everything to `logs/forward_test_predict_YYYYMMDD.log`

**Every day at 2:00 AM EST:**
1. Task runs automatically
2. Loads recent game results from database
3. Updates predictions with actual outcomes
4. Marks games as completed
5. Logs everything to `logs/forward_test_update_YYYYMMDD.log`

## 📊 Quick Commands

### View Task Status
```powershell
schtasks /Query /TN "NBA Forward Test - Predict"
schtasks /Query /TN "NBA Forward Test - Update"
```

### Run Tasks Manually (for testing)
```powershell
schtasks /Run /TN "NBA Forward Test - Predict"
schtasks /Run /TN "NBA Forward Test - Update"
```

### Check Logs
```powershell
# View today's prediction log
Get-Content logs\forward_test_predict_$(Get-Date -Format 'yyyyMMdd').log

# View today's update log
Get-Content logs\forward_test_update_$(Get-Date -Format 'yyyyMMdd').log

# View last 20 lines
Get-Content logs\forward_test_predict_$(Get-Date -Format 'yyyyMMdd').log -Tail 20
```

### Generate Performance Report
```powershell
poetry run python -m src.models.forward_test report
```

## ⚙️ Modify Schedule

### Change Times via Task Scheduler GUI
1. Press `Win + R`, type `taskschd.msc`, press Enter
2. Find "NBA Forward Test - Predict" or "NBA Forward Test - Update"
3. Right-click → Properties → Triggers tab
4. Edit trigger → Change time → OK

### Change Times via Command Line
```powershell
# Change prediction time to 5:00 PM EST (22:00 UTC)
schtasks /Change /TN "NBA Forward Test - Predict" /ST 22:00

# Change update time to 3:00 AM EST (08:00 UTC)
schtasks /Change /TN "NBA Forward Test - Update" /ST 08:00
```

## 🗑️ Remove Tasks (if needed)

```powershell
schtasks /Delete /TN "NBA Forward Test - Predict" /F
schtasks /Delete /TN "NBA Forward Test - Update" /F
```

Or use the script:
```powershell
.\scripts\schedule_forward_test_simple.ps1 -Remove
```

## ⚠️ Important Notes

1. **Computer must be on**: Tasks only run if your computer is awake
   - Consider preventing sleep during scheduled times
   - Or enable "Wake the computer to run this task" in Task Scheduler

2. **User must be logged in**: Tasks are set to "Interactive only" mode
   - Computer must be logged in (but can be locked)
   - To run when logged out, change to "Run whether user is logged on or not"

3. **Network required**: Tasks need internet for API calls
   - "Run only if network available" is enabled

4. **Time zones**: 
   - Tasks use UTC time internally
   - Windows converts to your local time zone
   - 11:00 PM UTC = 6:00 PM EST (winter) or 7:00 PM EDT (summer)

## 📈 Next Steps

1. ✅ **Tasks are created and ready**
2. ✅ **Wait for first automatic run** (tonight at 6 PM EST)
3. ✅ **Check logs tomorrow** to verify they're working
4. ✅ **Monitor for a few days** to ensure reliability
5. ✅ **Review weekly reports** to track performance

## 🎉 Success!

Your forward testing is now **fully automated**! The system will:
- Make predictions automatically every day before games
- Update results automatically every day after games
- Track everything in logs for monitoring
- Build your forward testing dataset automatically

No manual intervention needed - just check the logs and reports periodically!


