#!/usr/bin/env python3
"""
Diagnostic script for troubleshooting symbol mapping issues.

This script helps identify why a symbol cannot be resolved for a specific broker
and provides detailed information about available alternatives.

Usage:
    python symbol_diagnostics.py NIFTYNXT5025NOV35500CE dhan
    python symbol_diagnostics.py RELIANCE-EQ zerodha
"""

import sys
import os
import json
from pathlib import Path

# Add the services directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Import your modules
from brokers.symbol_map import debug_symbol_lookup, get_symbol_map, refresh_symbol_map
from services.webhook_receiver import WebhookEventSchema

def analyze_symbol(symbol: str, broker: str = "dhan", exchange: str = None):
    """Analyze a symbol and provide detailed diagnostic information."""
    
    print(f"🔍 Analyzing symbol: {symbol}")
    print(f"   Broker: {broker}")
    print(f"   Exchange: {exchange or 'auto-detect'}")
    print("=" * 60)
    
    # Test webhook normalization first
    print("\n1. WEBHOOK NORMALIZATION TEST")
    print("-" * 30)
    
    schema = WebhookEventSchema()
    test_data = {
        "user_id": 1,
        "symbol": symbol,
        "action": "BUY", 
        "qty": 1
    }
    
    try:
        normalized = schema.load(test_data)
        print(f"✅ Webhook normalization successful:")
        print(f"   Original: {symbol}")
        print(f"   Normalized: {normalized.get('symbol')}")
        print(f"   Exchange: {normalized.get('exchange')}")
        print(f"   Instrument Type: {normalized.get('instrument_type')}")
        print(f"   Strike: {normalized.get('strike')}")
        print(f"   Option Type: {normalized.get('option_type')}")
        print(f"   Expiry: {normalized.get('expiry')}")
        
        # Use normalized symbol for further analysis
        analysis_symbol = normalized.get('symbol', symbol)
        analysis_exchange = normalized.get('exchange', exchange)
        
    except Exception as e:
        print(f"❌ Webhook normalization failed: {e}")
        analysis_symbol = symbol
        analysis_exchange = exchange
    
    # Test symbol mapping
    print(f"\n2. SYMBOL MAPPING ANALYSIS")
    print("-" * 30)
    
    debug_info = debug_symbol_lookup(analysis_symbol, broker, analysis_exchange)
    
    print(f"Symbol variants tried: {debug_info['available_variants']}")
    print(f"Found in symbol map: {debug_info['found_mapping']}")
    
    if debug_info['found_mapping']:
        print(f"Available exchanges: {debug_info['available_exchanges']}")
        print(f"Available brokers: {debug_info['available_brokers']}")
        print(f"Broker data found: {bool(debug_info['broker_data'])}")
        
        if debug_info['broker_data']:
            print(f"Broker data: {json.dumps(debug_info['broker_data'], indent=2)}")
        else:
            print(f"❌ No data found for broker '{broker}'")
            if broker not in debug_info['available_brokers']:
                print(f"   Broker '{broker}' not available for this symbol")
                print(f"   Available brokers: {debug_info['available_brokers']}")
    else:
        print("❌ Symbol not found in mapping")
    
    print(f"\nDebug steps:")
    for i, step in enumerate(debug_info['debug_info'], 1):
        print(f"   {i}. {step}")
    
    # Check if symbol map needs refresh
    print(f"\n3. SYMBOL MAP REFRESH TEST")
    print("-" * 30)
    
    print("Refreshing symbol map...")
    try:
        refresh_symbol_map(force=True)
        print("✅ Symbol map refresh completed")
        
        # Re-test after refresh
        debug_info_after = debug_symbol_lookup(analysis_symbol, broker, analysis_exchange)
        
        if debug_info_after['found_mapping'] and debug_info_after['broker_data']:
            print("✅ Symbol found after refresh!")
            print(f"Broker data: {json.dumps(debug_info_after['broker_data'], indent=2)}")
        elif debug_info_after['found_mapping'] and not debug_info_after['broker_data']:
            print(f"⚠️  Symbol found but no broker data for '{broker}'")
            print(f"Available brokers: {debug_info_after['available_brokers']}")
        else:
            print("❌ Symbol still not found after refresh")
            
    except Exception as e:
        print(f"❌ Symbol map refresh failed: {e}")
    
    # Show similar symbols
    print(f"\n4. SIMILAR SYMBOLS SEARCH")
    print("-" * 30)
    
    symbol_map = get_symbol_map()
    similar_symbols = []
    
    # Extract root from the symbol for similarity search
    import re
    
    if analysis_symbol.endswith(("FUT", "CE", "PE")):
        root_match = re.match(r"^([A-Z0-9]+?)(\d{2})", analysis_symbol)
        if root_match:
            root = root_match.group(1)
            print(f"Searching for symbols starting with: {root}")
            
            for sym in symbol_map.keys():
                if sym.startswith(root) and sym != analysis_symbol:
                    similar_symbols.append(sym)
                    if len(similar_symbols) >= 10:  # Limit output
                        break
    
    if similar_symbols:
        print(f"Found {len(similar_symbols)} similar symbols:")
        for sim_sym in similar_symbols[:10]:
            print(f"   {sim_sym}")
            # Check if this similar symbol has the broker data
            debug_sim = debug_symbol_lookup(sim_sym, broker, analysis_exchange)
            if debug_sim['broker_data']:
                print(f"     ✅ Has {broker} data: {debug_sim['broker_data'].get('security_id', 'N/A')}")
            else:
                print(f"     ❌ No {broker} data")
    else:
        print("No similar symbols found")
    
    print(f"\n5. RECOMMENDATIONS")
    print("-" * 30)
    
    if not debug_info['found_mapping']:
        print("❌ Symbol not found in mapping. Possible causes:")
        print("   1. Symbol is expired or delisted")
        print("   2. Symbol format not recognized by normalization")
        print("   3. Symbol not available in broker instrument dumps")
        print("   4. Typo in symbol name")
        print("\n   Recommendations:")
        print("   - Check if the symbol is still active on the exchange")
        print("   - Verify the symbol format matches exchange conventions")
        print("   - Try using a similar symbol that was found above")
        
    elif not debug_info['broker_data']:
        print(f"⚠️  Symbol found but no {broker} data. Possible causes:")
        print(f"   1. {broker} doesn't support this symbol")
        print(f"   2. Symbol not in {broker}'s instrument dump")
        print(f"   3. Different symbol naming convention for {broker}")
        print(f"\n   Recommendations:")
        print(f"   - Try using a different broker: {debug_info['available_brokers']}")
        print(f"   - Check {broker}'s instrument dump for the correct symbol format")
        
    else:
        print("✅ Symbol mapping looks good!")
        print("   If you're still getting errors, check:")
        print("   1. Network connectivity to broker APIs")
        print("   2. Broker API credentials")
        print("   3. Symbol expiry dates")

def main():
    if len(sys.argv) < 2:
        print("Usage: python symbol_diagnostics.py <SYMBOL> [BROKER] [EXCHANGE]")
        print("\nExamples:")
        print("  python symbol_diagnostics.py NIFTYNXT5025NOV35500CE dhan")
        print("  python symbol_diagnostics.py RELIANCE-EQ zerodha NSE")
        sys.exit(1)
    
    symbol = sys.argv[1]
    broker = sys.argv[2] if len(sys.argv) > 2 else "dhan"
    exchange = sys.argv[3] if len(sys.argv) > 3 else None
    
    try:
        analyze_symbol(symbol, broker, exchange)
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Analysis failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
