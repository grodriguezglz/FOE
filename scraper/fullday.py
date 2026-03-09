#!/usr/bin/env python3
"""
MLB First Inning No-Run Statistics using MCP MLB Stats API
This script analyzes first inning performance to track games where no runs were scored.
"""

import pandas as pd
from datetime import datetime, timedelta
import json
import re
from collections import defaultdict

# Note: The MCP server would normally be running separately
# For direct usage, we'll import the underlying statsapi that MCP uses
try:
    import statsapi
    print("Using MLB-StatsAPI directly")
    USE_STATSAPI = True
except ImportError:
    print("MLB-StatsAPI not available")
    USE_STATSAPI = False

class MCPFirstInningAnalyzer:
    def __init__(self):
        self.games_data = []
        
    def parse_linescore_for_first_inning(self, linescore_text):
        """Enhanced parser for extracting first inning runs from linescore"""
        if not linescore_text:
            return None, None
            
        lines = linescore_text.split('\n')
        away_first_inning = None
        home_first_inning = None
        
        # Look for the line that contains inning-by-inning runs
        # Format is typically: Team | 1 2 3 4 5 6 7 8 9 | R H E
        data_lines = []
        for line in lines:
            if '|' in line and any(c.isdigit() for c in line):
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 3:  # Team name, innings, totals
                    innings_part = parts[1].strip()
                    # Check if this looks like inning data (has numbers)
                    if any(c.isdigit() for c in innings_part):
                        data_lines.append((parts[0], innings_part, parts[2:]))
        
        # Extract first inning runs for both teams
        if len(data_lines) >= 2:
            # Away team (first line)
            away_innings = data_lines[0][1].split()
            if away_innings and away_innings[0].replace('-', '0').isdigit():
                away_first_inning = int(away_innings[0].replace('-', '0'))
            
            # Home team (second line)
            home_innings = data_lines[1][1].split()
            if home_innings and home_innings[0].replace('-', '0').isdigit():
                home_first_inning = int(home_innings[0].replace('-', '0'))
        
        return away_first_inning, home_first_inning
    
    def get_game_details(self, game_id, game_date=None):
        """Get detailed game information including first inning data"""
        if not USE_STATSAPI:
            print(f"Cannot analyze game {game_id} - statsapi not available")
            return None
            
        try:
            # Get linescore and boxscore data
            linescore_text = statsapi.linescore(game_id)
            boxscore_data = statsapi.boxscore_data(game_id)
            
            # Parse first inning runs
            away_first, home_first = self.parse_linescore_for_first_inning(linescore_text)
            
            # Default to 0 if parsing failed
            if away_first is None:
                away_first = 0
            if home_first is None:
                home_first = 0
            
            # Get team names
            team_info = boxscore_data.get('teamInfo', {})
            away_team = team_info.get('away', {}).get('teamName', 'Unknown')
            home_team = team_info.get('home', {}).get('teamName', 'Unknown')
            
            result = {
                'game_id': game_id,
                'game_date': game_date,
                'away_team': away_team,
                'home_team': home_team,
                'away_first_inning_runs': away_first,
                'home_first_inning_runs': home_first,
                'no_runs_first_inning': (away_first == 0 and home_first == 0),
                'away_scoreless_first': away_first == 0,
                'home_scoreless_first': home_first == 0,
                'total_first_inning_runs': away_first + home_first,
                'raw_linescore': linescore_text  # For debugging
            }
            
            return result
            
        except Exception as e:
            print(f"Error analyzing game {game_id}: {e}")
            return None
    
    def get_games_for_period(self, start_date, end_date, team_id=None):
        """Get all games for a specific period"""
        if not USE_STATSAPI:
            print("Cannot fetch games - statsapi not available")
            return []
            
        try:
            if team_id:
                games = statsapi.schedule(
                    team=team_id,
                    start_date=start_date,
                    end_date=end_date
                )
                team_name = statsapi.lookup_team(team_id)[0]['name']
                print(f"Found {len(games)} games for {team_name}")
            else:
                games = statsapi.schedule(
                    start_date=start_date,
                    end_date=end_date
                )
                print(f"Found {len(games)} total games")
            
            # Filter to only completed games
            completed_games = [g for g in games if g.get('status') == 'Final']
            print(f"Found {len(completed_games)} completed games")
            
            return completed_games
            
        except Exception as e:
            print(f"Error fetching games: {e}")
            return []
    
    def analyze_period(self, start_date, end_date, team_id=None, max_games=None):
        """Analyze first inning statistics for a given period"""
        print(f"\n{'='*60}")
        print(f"ANALYZING FIRST INNING NO-RUN STATISTICS")
        print(f"Period: {start_date} to {end_date}")
        print(f"{'='*60}")
        
        games = self.get_games_for_period(start_date, end_date, team_id)
        
        if not games:
            print("No games found for analysis")
            return []
        
        # Limit games if specified
        if max_games and len(games) > max_games:
            games = games[:max_games]
            print(f"Limiting analysis to first {max_games} games")
        
        results = []
        
        for i, game in enumerate(games):
            game_date = game.get('game_date', 'Unknown')
            print(f"Analyzing game {i+1}/{len(games)} [{game_date}]: {game.get('away_name', 'Unknown')} vs {game.get('home_name', 'Unknown')}")
            
            analysis = self.get_game_details(game['game_id'], game_date)
            if analysis:
                results.append(analysis)
                
                # Show quick result
                if analysis['no_runs_first_inning']:
                    print(f"  [SCORELESS] No runs in 1st inning")
                else:
                    print(f"  [RUNS] Runs in 1st: {analysis['away_first_inning_runs']}-{analysis['home_first_inning_runs']}")
        
        return results
    
    def calculate_statistics(self, results):
        """Calculate comprehensive statistics from the results"""
        if not results:
            return {}
        
        df = pd.DataFrame(results)
        
        # Basic statistics
        total_games = len(results)
        no_runs_games = int(df['no_runs_first_inning'].sum())
        away_scoreless = int(df['away_scoreless_first'].sum())
        home_scoreless = int(df['home_scoreless_first'].sum())
        
        stats = {
            'total_games': total_games,
            'no_runs_first_inning_games': no_runs_games,
            'no_runs_first_inning_percentage': (no_runs_games / total_games) * 100,
            'away_team_scoreless_first': away_scoreless,
            'home_team_scoreless_first': home_scoreless,
            'away_team_scoreless_percentage': (away_scoreless / total_games) * 100,
            'home_team_scoreless_percentage': (home_scoreless / total_games) * 100,
            'average_first_inning_runs': df['total_first_inning_runs'].mean()
        }
        
        # Team-specific breakdown
        team_stats = defaultdict(lambda: {'games': 0, 'scoreless_first': 0, 'total_runs': 0})
        
        for result in results:
            # Away team stats
            away_team = result['away_team']
            team_stats[away_team]['games'] += 1
            team_stats[away_team]['total_runs'] += result['away_first_inning_runs']
            if result['away_scoreless_first']:
                team_stats[away_team]['scoreless_first'] += 1
            
            # Home team stats
            home_team = result['home_team']
            team_stats[home_team]['games'] += 1
            team_stats[home_team]['total_runs'] += result['home_first_inning_runs']
            if result['home_scoreless_first']:
                team_stats[home_team]['scoreless_first'] += 1
        
        # Calculate percentages and averages
        for team in team_stats:
            games = team_stats[team]['games']
            if games > 0:
                team_stats[team]['scoreless_percentage'] = (team_stats[team]['scoreless_first'] / games) * 100
                team_stats[team]['avg_first_inning_runs'] = team_stats[team]['total_runs'] / games
        
        stats['team_breakdown'] = dict(team_stats)
        
        return stats
    
    def print_detailed_report(self, stats, results=None):
        """Print a comprehensive report"""
        print(f"\n{'='*80}")
        print(f"FIRST INNING NO-RUN STATISTICS REPORT")
        print(f"{'='*80}")
        
        # Overall Statistics
        print(f"\nOVERALL STATISTICS:")
        print(f"   Total Games Analyzed: {stats['total_games']}")
        print(f"   Games with No Runs in First Inning: {stats['no_runs_first_inning_games']}")
        print(f"   Percentage of Scoreless First Innings: {stats['no_runs_first_inning_percentage']:.1f}%")
        print(f"   Average First Inning Runs per Game: {stats['average_first_inning_runs']:.2f}")
        
        # Home vs Away
        print(f"\nHOME vs AWAY BREAKDOWN:")
        print(f"   Away Teams - Scoreless First Innings: {stats['away_team_scoreless_first']} ({stats['away_team_scoreless_percentage']:.1f}%)")
        print(f"   Home Teams - Scoreless First Innings: {stats['home_team_scoreless_first']} ({stats['home_team_scoreless_percentage']:.1f}%)")
        
        # Team Rankings
        if stats.get('team_breakdown'):
            print(f"\nTEAM RANKINGS (by Scoreless First Inning Rate):")
            print(f"{'Team':<25} {'Games':<6} {'Scoreless':<9} {'Rate':<8} {'Avg Runs'}")
            print("-" * 65)
            
            sorted_teams = sorted(
                stats['team_breakdown'].items(),
                key=lambda x: x[1].get('scoreless_percentage', 0),
                reverse=True
            )
            
            for i, (team, data) in enumerate(sorted_teams[:10], 1):  # Top 10
                if data['games'] >= 3:  # Only teams with at least 3 games
                    print(f"{i:2d}. {team:<22} {data['games']:<6} {data['scoreless_first']:<9} "
                          f"{data.get('scoreless_percentage', 0):5.1f}%   {data.get('avg_first_inning_runs', 0):.2f}")
        
        # Sample Games
        if results:
            print(f"\nSAMPLE SCORELESS FIRST INNING GAMES:")
            scoreless_games = [r for r in results if r['no_runs_first_inning']][:5]
            for game in scoreless_games:
                print(f"   {game['game_date']}: {game['away_team']} @ {game['home_team']} (Game ID: {game['game_id']})")
                
        # Sample Games with Runs
        if results:
            runs_games = [r for r in results if not r['no_runs_first_inning']][:3]
            if runs_games:
                print(f"\nSAMPLE FIRST INNING SCORING GAMES:")
                for game in runs_games:
                    print(f"   {game['game_date']}: {game['away_team']} @ {game['home_team']} "
                          f"({game['away_first_inning_runs']}-{game['home_first_inning_runs']}) (Game ID: {game['game_id']})")

def main():
    """Main function to run the analysis"""
    analyzer = MCPFirstInningAnalyzer()
    
    print("MCP MLB First Inning No-Run Statistics Analyzer")
    print("=" * 50)
    
    # Configuration - modify these dates as needed
    end_date = datetime.now().strftime('%m/%d/%Y')
    
    # Try different time periods to find games
    time_periods = [
        ("Last 30 days", 30),
        ("Last 60 days", 60),
        ("Last 90 days", 90),
        ("2024 Season (Oct-Nov)", None)  # Special case
    ]
    
    results = []
    
    for period_name, days in time_periods:
        if days:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%m/%d/%Y')
        else:
            # 2024 season end
            start_date = "10/01/2024"
            end_date = "11/30/2024"
        
        print(f"\nTrying {period_name}: {start_date} to {end_date}")
        
        # Try to get games for this period
        results = analyzer.analyze_period(start_date, end_date, max_games=20)
        
        if results:
            print(f"Found {len(results)} games to analyze!")
            break
        else:
            print("No completed games found in this period")
    
    if results:
        # Calculate and display statistics
        stats = analyzer.calculate_statistics(results)
        analyzer.print_detailed_report(stats, results)
        
        # Save results to CSV for further analysis
        df = pd.DataFrame(results)
        filename = f"first_inning_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"\nResults saved to: {filename}")
        
    else:
        print("\nNo games found for analysis. This might be during off-season.")
        print("Try running during baseball season (March-October) for live data.")

def debug_single_game(game_id):
    """Debug function to examine a single game's linescore in detail"""
    if not USE_STATSAPI:
        print("statsapi not available")
        return
        
    try:
        print(f"=== DEBUGGING GAME {game_id} ===")
        
        # Get raw linescore
        linescore_text = statsapi.linescore(game_id)
        print(f"\nRAW LINESCORE:")
        print(linescore_text)
        print(f"\n" + "="*50)
        
        # Parse it step by step
        analyzer = MCPFirstInningAnalyzer()
        away_first, home_first = analyzer.parse_linescore_for_first_inning(linescore_text)
        
        print(f"PARSED RESULTS:")
        print(f"Away team first inning runs: {away_first}")
        print(f"Home team first inning runs: {home_first}")
        
        # Show the parsing process
        lines = linescore_text.split('\n')
        print(f"\nPARSING PROCESS:")
        for i, line in enumerate(lines):
            if '|' in line and any(c.isdigit() for c in line):
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 3:
                    print(f"Line {i}: Team='{parts[0]}', Innings='{parts[1]}', Totals='{parts[2:]}'")
                    innings = parts[1].strip().split()
                    if innings:
                        print(f"  First inning value: '{innings[0]}'")
        
        # Also get boxscore for comparison
        print(f"\nBOXSCORE DATA:")
        boxscore_data = statsapi.boxscore_data(game_id)
        team_info = boxscore_data.get('teamInfo', {})
        print(f"Away team: {team_info.get('away', {}).get('teamName', 'Unknown')}")
        print(f"Home team: {team_info.get('home', {}).get('teamName', 'Unknown')}")
        
    except Exception as e:
        print(f"Error debugging game: {e}")

def test_specific_game():
    """Test with the Padres vs Reds game you mentioned"""
    print("Let's debug the Padres vs Reds game...")
    
    # First, let's find that specific game
    analyzer = MCPFirstInningAnalyzer()
    games = analyzer.get_games_for_period("06/27/2025", "06/27/2025")
    
    padres_game = None
    for game in games:
        if ('Padres' in game.get('away_name', '') and 'Reds' in game.get('home_name', '')) or \
           ('Reds' in game.get('away_name', '') and 'Padres' in game.get('home_name', '')):
            padres_game = game
            break
    
    if padres_game:
        print(f"Found Padres vs Reds game: {padres_game['game_id']}")
        debug_single_game(padres_game['game_id'])
    else:
        print("Could not find Padres vs Reds game")
        print("Available games on 06/27/2025:")
        for game in games[:5]:
            print(f"  {game.get('away_name')} vs {game.get('home_name')} (ID: {game['game_id']})")

def comprehensive_debug_day(date):
    """Debug ALL games for a specific day with raw data dump"""
    if not USE_STATSAPI:
        print("statsapi not available")
        return
        
    print(f"=== COMPREHENSIVE DEBUG FOR {date} ===")
    
    try:
        # Get all games for this date
        games = statsapi.schedule(start_date=date, end_date=date)
        completed_games = [g for g in games if g.get('status') == 'Final']
        
        print(f"Found {len(completed_games)} completed games on {date}")
        print("="*80)
        
        analyzer = MCPFirstInningAnalyzer()
        
        for i, game in enumerate(completed_games, 1):
            game_id = game['game_id']
            away_team = game.get('away_name', 'Unknown')
            home_team = game.get('home_name', 'Unknown')
            
            print(f"\nGAME {i}: {away_team} @ {home_team}")
            print(f"Game ID: {game_id}")
            print(f"Date: {game.get('game_date', 'Unknown')}")
            print("-" * 60)
            
            try:
                # Get raw linescore
                linescore_text = statsapi.linescore(game_id)
                print(f"RAW LINESCORE:")
                print(linescore_text)
                print()
                
                # Parse it
                away_first, home_first = analyzer.parse_linescore_for_first_inning(linescore_text)
                print(f"PARSED FIRST INNING:")
                print(f"  Away ({away_team}): {away_first} runs")
                print(f"  Home ({home_team}): {home_first} runs")
                print(f"  No runs: {away_first == 0 and home_first == 0}")
                
                # Show parsing details
                print(f"\nPARSING BREAKDOWN:")
                lines = linescore_text.split('\n')
                data_lines_found = []
                
                for line_num, line in enumerate(lines):
                    if '|' in line and any(c.isdigit() for c in line):
                        parts = [p.strip() for p in line.split('|')]
                        if len(parts) >= 3:
                            data_lines_found.append((line_num, parts))
                            print(f"  Line {line_num}: '{line}'")
                            print(f"    Split: {parts}")
                            innings = parts[1].strip().split()
                            if innings:
                                print(f"    Innings: {innings}")
                                print(f"    First inning value: '{innings[0]}'")
                
                print(f"  Total data lines found: {len(data_lines_found)}")
                
                # Also try to get boxscore for additional context
                try:
                    boxscore_data = statsapi.boxscore_data(game_id)
                    team_info = boxscore_data.get('teamInfo', {})
                    actual_away = team_info.get('away', {}).get('teamName', 'Unknown')
                    actual_home = team_info.get('home', {}).get('teamName', 'Unknown')
                    print(f"\nBOXSCORE TEAM NAMES:")
                    print(f"  Away: {actual_away}")
                    print(f"  Home: {actual_home}")
                except Exception as e:
                    print(f"  Error getting boxscore: {e}")
                
            except Exception as e:
                print(f"ERROR analyzing game {game_id}: {e}")
            
            print("="*80)
            
            # Add a pause every 5 games to make it readable
            if i % 5 == 0 and i < len(completed_games):
                input(f"\nPress Enter to continue with next 5 games... ({i}/{len(completed_games)} done)")
        
        print(f"\nDEBUG COMPLETE: Analyzed {len(completed_games)} games")
        
    except Exception as e:
        print(f"Error in comprehensive debug: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_specific_game()
        elif sys.argv[1] == "debug" and len(sys.argv) > 2:
            debug_single_game(sys.argv[2])
        elif sys.argv[1] == "fullday" and len(sys.argv) > 2:
            comprehensive_debug_day(sys.argv[2])
        else:
            print("Usage:")
            print("  python script.py                    # Run normal analysis")
            print("  python script.py test               # Test Padres vs Reds game")
            print("  python script.py debug <game_id>    # Debug specific game")
            print("  python script.py fullday <date>     # Debug all games on date (MM/DD/YYYY)")
    else:
        main()