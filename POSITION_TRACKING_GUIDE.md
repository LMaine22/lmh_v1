# Position Tracking CLI - Quick Guide

## 🚀 Run the CLI:

```bash
cd "/Users/lutherhart/Library/Mobile Documents/com~apple~CloudDocs/Quant Projects/lmhv_01"
python run_loop_cli.py
```

## 📋 On Startup - You'll Be Asked:

```
📋 Do you have an existing position? (y/n): y

🔧 Enter your position details:
   Type (CALL/PUT): CALL
   Contract symbol (e.g., O:QQQ251110C00620000): O:QQQ251110C00620000
   Strike price: $620
   Entry price per contract: $0.72
   Quantity (contracts): 1

   📊 Fetching current data to initialize tracking...
   
   ✅ Position loaded:
      CALL O:QQQ251110C00620000
      Strike: $620.00
      Entry: $0.72
      Quantity: 1
      Risk: $72.00
      Current spot: $621.55
      Current pressure: 3.2×
```

## 💼 Then You'll See Position Monitoring:

```
13:20:00 ET | 💼 HOLDING CALL     | P&L: +150.0% ($ +108.00) | Peak: +150% | Spot: $ 621.55 | Pressure:  3.2× BULLISH
         Current: $1.80 | Entry: $0.72 | Trail: $1.35 | Time: 60m

13:20:15 ET | 💼 HOLDING CALL     | P&L: +166.7% ($ +120.00) | Peak: +167% | Spot: $ 622.10 | Pressure:  4.1× BULLISH
         Current: $1.92 | Entry: $0.72 | Trail: $1.44 | Time: 61m
```

## 🚨 Auto-Exit Triggers for Your Position:

Since you're **up +150%**, the system will auto-exit if:

1. **Pressure drops below 3.0×** 
   ```
   🏁 EXITED CALL
   Reason: 💎 PRESSURE FADING: Up +150% but pressure dropped to 2.8×
   ```

2. **Drops 25% from peak** (currently would be $1.35)
   ```
   🏁 EXITED CALL
   Reason: 📉 TRAILING STOP: Dropped 25% from peak +150%
   ```

3. **Direction flips to BEARISH**
   ```
   🏁 EXITED CALL
   Reason: 🔄 PRESSURE REVERSAL: BULLISH → BEARISH
   ```

4. **Gets within 30 minutes of close** (2:30 PM)
   ```
   🏁 EXITED CALL
   Reason: ⏰ PROTECT GAINS: +150% with 28min left
   ```

## 🎯 Your Position Details to Enter:

**For your actual trade:**
- Type: **CALL**
- Contract: **O:QQQ251110C00620000**
- Strike: **620**
- Entry: **0.72**
- Quantity: **1**

## ⚙️ The System Will:

✅ Track your P&L in real-time  
✅ Show trailing stop price  
✅ Monitor pressure/direction  
✅ Auto-alert on exit triggers  
✅ Update peak P&L  

---

**Run it now and enter your position!** 🚀

```bash
python run_loop_cli.py
```

