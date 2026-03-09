#!/usr/bin/env python3
"""
MLB First Inning No-Run Statistics using MCP MLB Stats API
This script analyzes first inning performance and predicts NRFI for upcoming games.
"""

import pandas as pd
from datetime import datetime, timedelta
import json
import re
import pickle
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
        
        # Look for lines with team data (not header lines)
        team_lines = []
        for line in lines:
            # Skip header lines and empty lines
            if not line.strip() or 'Final' in line or line.strip().startswith('1 2 3'):
                continue
                
            # Look for lines that have team names and inning data
            # Format: "TeamName 0 0 0 0 0 0 0 0 1  1   1   1"
            parts = line.strip().split()
            if len(parts) >= 10:  # Team name + 9 innings + totals
                team_name = parts[0]
                innings = parts[1:10]  # First 9 elements after team name are innings
                
                # Check if this looks like inning data (all digits)
                if all(part.isdigit() for part in innings):
                    first_inning_runs = int(innings[0])  # First inning is index 0
                    team_lines.append((team_name, first_inning_runs, line))
        
        # Assign first two valid team lines as away and home
        if len(team_lines) >= 2:
            away_first_inning = team_lines[0][1]  # First team is away
            home_first_inning = team_lines[1][1]  # Second team is home
        elif len(team_lines) == 1:
            # Only one team found - this shouldn't happen but handle it
            away_first_inning = team_lines[0][1]
            home_first_inning = 0
        
        return away_first_inning, home_first_inning
    
    def get_starting_pitcher_info(self, boxscore_data, side):
        """Extract starting pitcher information from boxscore data"""
        try:
            # Get pitchers data for the specified side (away/home)
            pitchers_data = boxscore_data.get(side, {}).get('pitchers', [])
            
            if not pitchers_data:
                return {'name': 'Unknown', 'id': None}
            
            # Starting pitcher is typically the first pitcher listed
            starting_pitcher_id = pitchers_data[0] if pitchers_data else None
            
            if starting_pitcher_id:
                # Get pitcher details from player info
                player_key = f"ID{starting_pitcher_id}"
                player_info = boxscore_data.get('playerInfo', {}).get(player_key, {})
                
                pitcher_name = player_info.get('fullName', 'Unknown')
                
                return {
                    'name': pitcher_name,
                    'id': starting_pitcher_id
                }
            else:
                return {'name': 'Unknown', 'id': None}
                
        except Exception as e:
            print(f"Error getting pitcher info for {side}: {e}")
            return {'name': 'Unknown', 'id': None}
    
    def get_game_details(self, game_id, game_date=None):
        """Get detailed game information including first inning data and pitcher info"""
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
            
            # Get starting pitcher information
            away_pitcher_info = self.get_starting_pitcher_info(boxscore_data, 'away')
            home_pitcher_info = self.get_starting_pitcher_info(boxscore_data, 'home')
            
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
                'away_starting_pitcher': away_pitcher_info['name'],
                'away_pitcher_id': away_pitcher_info['id'],
                'home_starting_pitcher': home_pitcher_info['name'],
                'home_pitcher_id': home_pitcher_info['id'],
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
    
    def get_upcoming_games(self, date):
        """Get scheduled games for a specific date"""
        if not USE_STATSAPI:
            print("Cannot fetch upcoming games - statsapi not available")
            return []
            
        try:
            games = statsapi.schedule(start_date=date, end_date=date)
            # Filter for scheduled games (not completed)
            upcoming_games = [g for g in games if g.get('status') not in ['Final', 'Completed']]
            
            print(f"Found {len(upcoming_games)} upcoming games on {date}")
            return upcoming_games
            
        except Exception as e:
            print(f"Error fetching upcoming games: {e}")
            return []
    
    def analyze_period(self, start_date, end_date, team_id=None, max_games=None, quiet_mode=False):
        """Analyze first inning statistics for a given period"""
        if not quiet_mode:
            print(f"\n{'='*60}")
            print(f"ANALYZING FIRST INNING NO-RUN STATISTICS")
            print(f"Period: {start_date} to {end_date}")
            print(f"{'='*60}")
        
        games = self.get_games_for_period(start_date, end_date, team_id)
        
        if not games:
            if not quiet_mode:
                print("No games found for analysis")
            return []
        
        # Limit games if specified
        if max_games and len(games) > max_games:
            games = games[:max_games]
            if not quiet_mode:
                print(f"Limiting analysis to first {max_games} games")
        
        results = []
        total_games = len(games)
        
        # Progress indicators for large datasets
        if quiet_mode and total_games > 100:
            print(f"Processing {total_games} games in quiet mode...")
            progress_interval = max(50, total_games // 20)  # Show progress every 5%
        
        for i, game in enumerate(games):
            # Show progress for large datasets
            if quiet_mode and total_games > 100:
                if (i + 1) % progress_interval == 0 or i == 0:
                    percentage = ((i + 1) / total_games) * 100
                    print(f"Progress: {i+1}/{total_games} games ({percentage:.1f}%)")
            elif not quiet_mode:
                game_date = game.get('game_date', 'Unknown')
                print(f"Analyzing game {i+1}/{len(games)} [{game_date}]: {game.get('away_name', 'Unknown')} vs {game.get('home_name', 'Unknown')}")
            
            analysis = self.get_game_details(game['game_id'], game.get('game_date'))
            if analysis:
                results.append(analysis)
                
                # Show quick result only in verbose mode
                if not quiet_mode:
                    if analysis['no_runs_first_inning']:
                        print(f"  [SCORELESS] No runs in 1st inning")
                    else:
                        print(f"  [RUNS] Runs in 1st: {analysis['away_first_inning_runs']}-{analysis['home_first_inning_runs']}")
        
        if quiet_mode and total_games > 100:
            print(f"Completed processing {len(results)} games successfully.")
        
        return results
    
    def get_pitcher_historical_nrfi_rate(self, pitcher_name, pitcher_stats):
        """Get historical NRFI rate for a specific pitcher"""
        if pitcher_name in pitcher_stats:
            data = pitcher_stats[pitcher_name]
            if data['games'] >= 3:  # Minimum sample size
                return data.get('scoreless_percentage', 0) / 100
        
        # Default rate if no historical data (league average)
        return 0.65  # Approximate league average NRFI rate
    
    def get_team_offensive_first_inning_rate(self, team_name, team_stats):
        """Get team's tendency to score in first inning"""
        if team_name in team_stats:
            data = team_stats[team_name]
            if data['games'] >= 5:  # Minimum sample size
                # Convert scoreless rate to scoring rate
                scoring_rate = 1 - (data.get('scoreless_percentage', 65) / 100)
                return scoring_rate
        
        # Default rate if no historical data
        return 0.35  # Approximate league average first inning scoring rate
    
    def predict_nrfi_for_game(self, game, pitcher_stats, team_stats):
        """Predict NRFI probability for a single game"""
        away_team = game.get('away_name', 'Unknown')
        home_team = game.get('home_name', 'Unknown')
        
        # Try to get starting pitchers from schedule data
        away_pitcher = game.get('away_probable_pitcher', 'Unknown')
        home_pitcher = game.get('home_probable_pitcher', 'Unknown')
        
        # If not available in schedule, use placeholder
        if away_pitcher == 'Unknown' or not away_pitcher:
            away_pitcher = f"{away_team} Starter"
        if home_pitcher == 'Unknown' or not home_pitcher:
            home_pitcher = f"{home_team} Starter"
        
        # Get pitcher NRFI rates (probability they throw scoreless first)
        away_pitcher_nrfi_rate = self.get_pitcher_historical_nrfi_rate(away_pitcher, pitcher_stats)
        home_pitcher_nrfi_rate = self.get_pitcher_historical_nrfi_rate(home_pitcher, pitcher_stats)
        
        # Get team offensive rates (probability they score in first)
        away_team_scoring_rate = self.get_team_offensive_first_inning_rate(away_team, team_stats)
        home_team_scoring_rate = self.get_team_offensive_first_inning_rate(home_team, team_stats)
        
        # Calculate NRFI probability
        away_no_score_prob = home_pitcher_nrfi_rate * (1 - away_team_scoring_rate * 0.5)
        home_no_score_prob = away_pitcher_nrfi_rate * (1 - home_team_scoring_rate * 0.5)
        
        nrfi_probability = away_no_score_prob * home_no_score_prob
        
        # Apply adjustments
        if 'home' in game.get('venue', '').lower():
            nrfi_probability *= 1.02
        
        # Cap probability between 10% and 90%
        nrfi_probability = max(0.10, min(0.90, nrfi_probability))
        
        return {
            'game_id': game.get('game_pk', 'Unknown'),
            'game_time': game.get('game_date', 'Unknown'),
            'away_team': away_team,
            'home_team': home_team,
            'away_pitcher': away_pitcher,
            'home_pitcher': home_pitcher,
            'away_pitcher_nrfi_rate': away_pitcher_nrfi_rate,
            'home_pitcher_nrfi_rate': home_pitcher_nrfi_rate,
            'predicted_nrfi_probability': nrfi_probability,
            'confidence_level': self.get_confidence_level(nrfi_probability, away_pitcher, home_pitcher, pitcher_stats),
            'venue': game.get('venue', {}).get('name', 'Unknown')
        }
    
    def get_confidence_level(self, probability, away_pitcher, home_pitcher, pitcher_stats):
        """Determine confidence level based on probability and data quality"""
        # Check if we have good historical data for both pitchers
        away_has_data = away_pitcher in pitcher_stats and pitcher_stats[away_pitcher]['games'] >= 5
        home_has_data = home_pitcher in pitcher_stats and pitcher_stats[home_pitcher]['games'] >= 5
        
        if probability >= 0.70:
            confidence = "HIGH" if (away_has_data and home_has_data) else "MEDIUM"
        elif probability >= 0.55:
            confidence = "MEDIUM"
        elif probability >= 0.40:
            confidence = "LOW-MEDIUM"
        else:
            confidence = "LOW"
        
        return confidence
    
    def predict_tomorrows_nrfi(self, historical_stats=None, target_date=None):
        """Predict NRFI for tomorrow's games using historical data"""
        if target_date is None:
            # Default to tomorrow
            tomorrow = datetime.now() + timedelta(days=1)
            target_date = tomorrow.strftime('%m/%d/%Y')
        
        print(f"PREDICTING NRFI FOR {target_date}")
        print("="*60)
        
        # Get upcoming games
        upcoming_games = self.get_upcoming_games(target_date)
        
        if not upcoming_games:
            print(f"No upcoming games found for {target_date}")
            return []
        
        # If no historical stats provided, use empty dicts (will use defaults)
        if historical_stats is None:
            pitcher_stats = {}
            team_stats = {}
        else:
            pitcher_stats = historical_stats.get('pitcher_breakdown', {})
            team_stats = historical_stats.get('team_breakdown', {})
        
        predictions = []
        
        print(f"Analyzing {len(upcoming_games)} upcoming games...")
        
        for game in upcoming_games:
            prediction = self.predict_nrfi_for_game(game, pitcher_stats, team_stats)
            predictions.append(prediction)
        
        # Sort by NRFI probability (highest first)
        predictions.sort(key=lambda x: x['predicted_nrfi_probability'], reverse=True)
        
        return predictions
    
    def print_nrfi_predictions(self, predictions):
        """Print formatted NRFI predictions"""
        if not predictions:
            print("No predictions available")
            return
        
        print(f"\nNRFI PREDICTIONS - TOP RECOMMENDATIONS")
        print("="*90)
        
        # High confidence predictions
        high_conf_predictions = [p for p in predictions if p['confidence_level'] == 'HIGH']
        if high_conf_predictions:
            print(f"\nHIGH CONFIDENCE NRFI PICKS:")
            print(f"{'Rank':<4} {'Matchup':<35} {'NRFI %':<8} {'Away Pitcher':<20} {'Home Pitcher'}")
            print("-" * 90)
            
            for i, pred in enumerate(high_conf_predictions[:5], 1):
                matchup = f"{pred['away_team']} @ {pred['home_team']}"
                print(f"{i:<4} {matchup:<35} {pred['predicted_nrfi_probability']*100:5.1f}%   "
                      f"{pred['away_pitcher']:<20} {pred['home_pitcher']}")
        
        # All predictions ranked
        print(f"\nALL GAMES RANKED BY NRFI PROBABILITY:")
        print(f"{'Rank':<4} {'Matchup':<35} {'NRFI %':<8} {'Confidence':<12} {'Game Time'}")
        print("-" * 90)
        
        for i, pred in enumerate(predictions, 1):
            matchup = f"{pred['away_team']} @ {pred['home_team']}"
            game_time = pred['game_time'].split('T')[1][:5] if 'T' in pred['game_time'] else pred['game_time']
            
            print(f"{i:<4} {matchup:<35} {pred['predicted_nrfi_probability']*100:5.1f}%   "
                  f"{pred['confidence_level']:<12} {game_time}")
        
        # Summary
        high_prob_games = [p for p in predictions if p['predicted_nrfi_probability'] >= 0.65]
        print(f"\nSUMMARY:")
        print(f"Total games analyzed: {len(predictions)}")
        print(f"High probability NRFI games (≥65%): {len(high_prob_games)}")
        print(f"Average NRFI probability: {sum(p['predicted_nrfi_probability'] for p in predictions) / len(predictions) * 100:.1f}%")
    
    def calculate_statistics(self, results):
        """Calculate comprehensive statistics from the results including pitcher analysis"""
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
        
        # Calculate percentages and averages for teams
        for team in team_stats:
            games = team_stats[team]['games']
            if games > 0:
                team_stats[team]['scoreless_percentage'] = (team_stats[team]['scoreless_first'] / games) * 100
                team_stats[team]['avg_first_inning_runs'] = team_stats[team]['total_runs'] / games
        
        stats['team_breakdown'] = dict(team_stats)
        
        # Pitcher-specific analysis
        pitcher_stats = defaultdict(lambda: {
            'games': 0, 'runs_allowed': 0, 'scoreless_first': 0, 'team': None
        })
        
        for result in results:
            # Away pitcher (pitching to home team)
            away_pitcher = result['away_starting_pitcher']
            if away_pitcher != 'Unknown':
                pitcher_stats[away_pitcher]['games'] += 1
                pitcher_stats[away_pitcher]['runs_allowed'] += result['home_first_inning_runs']
                pitcher_stats[away_pitcher]['team'] = result['away_team']
                if result['home_first_inning_runs'] == 0:
                    pitcher_stats[away_pitcher]['scoreless_first'] += 1
            
            # Home pitcher (pitching to away team)
            home_pitcher = result['home_starting_pitcher']
            if home_pitcher != 'Unknown':
                pitcher_stats[home_pitcher]['games'] += 1
                pitcher_stats[home_pitcher]['runs_allowed'] += result['away_first_inning_runs']
                pitcher_stats[home_pitcher]['team'] = result['home_team']
                if result['away_first_inning_runs'] == 0:
                    pitcher_stats[home_pitcher]['scoreless_first'] += 1
        
        # Calculate pitcher percentages and averages
        for pitcher in pitcher_stats:
            games = pitcher_stats[pitcher]['games']
            if games > 0:
                pitcher_stats[pitcher]['scoreless_percentage'] = (
                    pitcher_stats[pitcher]['scoreless_first'] / games
                ) * 100
                pitcher_stats[pitcher]['avg_runs_allowed'] = (
                    pitcher_stats[pitcher]['runs_allowed'] / games
                )
        
        stats['pitcher_breakdown'] = dict(pitcher_stats)
        
        return stats
    
    def calculate_season_summary(self, stats):
        """Calculate season-level summary statistics"""
        season_summary = {
            'league_scoreless_rate': stats['no_runs_first_inning_percentage'],
            'league_avg_first_inning_runs': stats['average_first_inning_runs'],
            'home_field_advantage': stats['home_team_scoreless_percentage'] - stats['away_team_scoreless_percentage'],
            'total_games_analyzed': stats['total_games']
        }
        
        # Best and worst teams
        if stats.get('team_breakdown'):
            teams_sorted = sorted(
                stats['team_breakdown'].items(),
                key=lambda x: x[1].get('scoreless_percentage', 0),
                reverse=True
            )
            
            # Filter teams with at least 3 games
            qualified_teams = [(team, data) for team, data in teams_sorted if data['games'] >= 3]
            
            if qualified_teams:
                season_summary['best_first_inning_defense'] = qualified_teams[0]
                season_summary['worst_first_inning_defense'] = qualified_teams[-1]
        
        # Best and worst pitchers
        if stats.get('pitcher_breakdown'):
            pitchers_sorted = sorted(
                stats['pitcher_breakdown'].items(),
                key=lambda x: x[1].get('scoreless_percentage', 0),
                reverse=True
            )
            
            # Filter pitchers with at least 2 starts
            qualified_pitchers = [(pitcher, data) for pitcher, data in pitchers_sorted if data['games'] >= 2]
            
            if qualified_pitchers:
                season_summary['best_first_inning_pitcher'] = qualified_pitchers[0]
                season_summary['worst_first_inning_pitcher'] = qualified_pitchers[-1]
        
        return season_summary
    
    def print_detailed_report(self, stats, results=None):
        """Print a comprehensive report including season and pitcher analysis"""
        print(f"\n{'='*80}")
        print(f"FIRST INNING NO-RUN STATISTICS REPORT")
        print(f"{'='*80}")
        
        # Season Summary
        season_summary = self.calculate_season_summary(stats)
        print(f"\nSEASON SUMMARY:")
        print(f"   League Scoreless First Inning Rate: {season_summary['league_scoreless_rate']:.1f}%")
        print(f"   League Average First Inning Runs: {season_summary['league_avg_first_inning_runs']:.2f}")
        print(f"   Home Field Advantage: {season_summary['home_field_advantage']:+.1f}% (scoreless rate difference)")
        print(f"   Total Games Analyzed: {season_summary['total_games_analyzed']}")
        
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
        
        # Best/Worst Teams from Season Summary
        if season_summary.get('best_first_inning_defense'):
            best_team, best_data = season_summary['best_first_inning_defense']
            print(f"   Best First Inning Defense: {best_team} ({best_data['scoreless_percentage']:.1f}% scoreless)")
        
        if season_summary.get('worst_first_inning_defense'):
            worst_team, worst_data = season_summary['worst_first_inning_defense']
            print(f"   Worst First Inning Defense: {worst_team} ({worst_data['scoreless_percentage']:.1f}% scoreless)")
        
        # Team Rankings
        if stats.get('team_breakdown'):
            print(f"\nTEAM RANKINGS (by Scoreless First Inning Rate):")
            print(f"{'Rank':<4} {'Team':<25} {'Games':<6} {'Scoreless':<9} {'Rate':<8} {'Avg Runs'}")
            print("-" * 70)
            
            sorted_teams = sorted(
                stats['team_breakdown'].items(),
                key=lambda x: x[1].get('scoreless_percentage', 0),
                reverse=True
            )
            
            for i, (team, data) in enumerate(sorted_teams[:15], 1):  # Top 15
                if data['games'] >= 3:  # Only teams with at least 3 games
                    print(f"{i:2d}. {team:<24} {data['games']:<6} {data['scoreless_first']:<9} "
                          f"{data.get('scoreless_percentage', 0):5.1f}%   {data.get('avg_first_inning_runs', 0):.2f}")
        
        # Pitcher Analysis
        if stats.get('pitcher_breakdown'):
            print(f"\nSTARTING PITCHER ANALYSIS:")
            
            # Best pitchers (highest scoreless rate)
            pitcher_sorted = sorted(
                stats['pitcher_breakdown'].items(),
                key=lambda x: x[1].get('scoreless_percentage', 0),
                reverse=True
            )
            
            qualified_pitchers = [(p, d) for p, d in pitcher_sorted if d['games'] >= 2]
            
            if qualified_pitchers:
                print(f"\nTOP FIRST INNING PITCHERS (min 2 starts):")
                print(f"{'Rank':<4} {'Pitcher':<25} {'Team':<15} {'Starts':<7} {'Scoreless':<9} {'Rate':<8} {'Avg RA'}")
                print("-" * 85)
                
                for i, (pitcher, data) in enumerate(qualified_pitchers[:10], 1):  # Top 10
                    print(f"{i:2d}. {pitcher:<24} {data['team']:<15} {data['games']:<7} "
                          f"{data['scoreless_first']:<9} {data.get('scoreless_percentage', 0):5.1f}%   "
                          f"{data.get('avg_runs_allowed', 0):.2f}")
                
                # Worst pitchers
                print(f"\nWORST FIRST INNING PITCHERS (min 2 starts):")
                print(f"{'Rank':<4} {'Pitcher':<25} {'Team':<15} {'Starts':<7} {'Runs Allowed':<12} {'Rate'}")
                print("-" * 85)
                
                worst_pitchers = qualified_pitchers[-5:]  # Bottom 5
                for i, (pitcher, data) in enumerate(reversed(worst_pitchers), 1):
                    print(f"{i:2d}. {pitcher:<24} {data['team']:<15} {data['games']:<7} "
                          f"{data['runs_allowed']:<12} {data.get('scoreless_percentage', 0):5.1f}%")
        
        # Sample Games
        if results:
            print(f"\nSAMPLE SCORELESS FIRST INNING GAMES:")
            scoreless_games = [r for r in results if r['no_runs_first_inning']][:5]
            for game in scoreless_games:
                print(f"   {game['game_date']}: {game['away_team']} @ {game['home_team']}")
                print(f"      Pitchers: {game['away_starting_pitcher']} vs {game['home_starting_pitcher']}")
                
        # Sample Games with Runs
        if results:
            runs_games = [r for r in results if not r['no_runs_first_inning']][:3]
            if runs_games:
                print(f"\nSAMPLE FIRST INNING SCORING GAMES:")
                for game in runs_games:
                    print(f"   {game['game_date']}: {game['away_team']} @ {game['home_team']} "
                          f"({game['away_first_inning_runs']}-{game['home_first_inning_runs']})")
                    print(f"      Pitchers: {game['away_starting_pitcher']} vs {game['home_starting_pitcher']}")

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
                
            except Exception as e:
                print(f"ERROR analyzing game {game_id}: {e}")
            
            print("="*80)
            
            # Add a pause every 5 games to make it readable
            if i % 5 == 0 and i < len(completed_games):
                input(f"\nPress Enter to continue with next 5 games... ({i}/{len(completed_games)} done)")
        
        print(f"\nDEBUG COMPLETE: Analyzed {len(completed_games)} games")
        
    except Exception as e:
        print(f"Error in comprehensive debug: {e}")

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

def analyze_season(start_date, end_date, max_games=None, quiet_mode=True):
    """Analyze an entire season or large date range with quiet mode for efficiency"""
    print(f"SEASON ANALYSIS: {start_date} to {end_date}")
    if quiet_mode:
        print("Running in QUIET MODE - minimal output during processing")
    print("="*60)
    
    analyzer = MCPFirstInningAnalyzer()
    
    try:
        results = analyzer.analyze_period(start_date, end_date, max_games=max_games, quiet_mode=quiet_mode)
        
        if results:
            print(f"\nCalculating statistics for {len(results)} games...")
            stats = analyzer.calculate_statistics(results)
            
            print("Generating comprehensive report...")
            analyzer.print_detailed_report(stats, results)
            
            # Save comprehensive season data
            print("Saving detailed game data...")
            df = pd.DataFrame(results)
            season_filename = f"season_first_inning_analysis_{start_date.replace('/', '')}_{end_date.replace('/', '')}.csv"
            df.to_csv(season_filename, index=False)
            print(f"Season results saved to: {season_filename}")
            
            # Save pitcher summary
            print("Saving pitcher analysis...")
            pitcher_df = pd.DataFrame([
                {
                    'pitcher': pitcher,
                    'team': data['team'],
                    'games': data['games'],
                    'runs_allowed': data['runs_allowed'],
                    'scoreless_first': data['scoreless_first'],
                    'scoreless_percentage': data.get('scoreless_percentage', 0),
                    'avg_runs_allowed': data.get('avg_runs_allowed', 0)
                }
                for pitcher, data in stats.get('pitcher_breakdown', {}).items()
                if data['games'] >= 1
            ])
            
            pitcher_filename = f"pitcher_first_inning_analysis_{start_date.replace('/', '')}_{end_date.replace('/', '')}.csv"
            pitcher_df.to_csv(pitcher_filename, index=False)
            print(f"Pitcher analysis saved to: {pitcher_filename}")
            
            # Save team summary
            print("Saving team analysis...")
            team_df = pd.DataFrame([
                {
                    'team': team,
                    'games': data['games'],
                    'scoreless_first': data['scoreless_first'],
                    'total_runs': data['total_runs'],
                    'scoreless_percentage': data.get('scoreless_percentage', 0),
                    'avg_first_inning_runs': data.get('avg_first_inning_runs', 0)
                }
                for team, data in stats.get('team_breakdown', {}).items()
            ])
            
            team_filename = f"team_first_inning_analysis_{start_date.replace('/', '')}_{end_date.replace('/', '')}.csv"
            team_df.to_csv(team_filename, index=False)
            print(f"Team analysis saved to: {team_filename}")
            
            # Save stats for predictions
            with open('latest_season_stats.pkl', 'wb') as f:
                pickle.dump(stats, f)
            print("Season statistics saved for future predictions")
            
            print(f"\nSEASON ANALYSIS COMPLETE!")
            print(f"Files created:")
            print(f"  - {season_filename} (all games)")
            print(f"  - {pitcher_filename} (pitcher stats)")
            print(f"  - {team_filename} (team stats)")
            print(f"  - latest_season_stats.pkl (for predictions)")
            
            return stats, results
        else:
            print("No games found for season analysis")
            return None, None
            
    except Exception as e:
        print(f"Error in season analysis: {e}")
        return None, None

def predict_tomorrow(historical_data_file=None, target_date=None):
    """Predict NRFI for tomorrow's games using historical analysis"""
    analyzer = MCPFirstInningAnalyzer()
    
    # Load saved stats
    try:
        with open('latest_season_stats.pkl', 'rb') as f:
            historical_stats = pickle.load(f)
        print("Using saved season data for predictions")
    except:
        historical_stats = None
        print("No saved data found, using defaults")
    
    # Get predictions
    predictions = analyzer.predict_tomorrows_nrfi(historical_stats, target_date)
    
    if predictions:
        # Print formatted predictions
        analyzer.print_nrfi_predictions(predictions)
        
        # Save predictions to CSV
        predictions_df = pd.DataFrame(predictions)
        date_str = target_date.replace('/', '') if target_date else "tomorrow"
        filename = f"nrfi_predictions_{date_str}.csv"
        predictions_df.to_csv(filename, index=False)
        print(f"\nPredictions saved to: {filename}")
        
        return predictions
    else:
        print("No predictions could be generated")
        return []

def predict_with_season_data(season_stats, target_date=None):
    """Predict NRFI using existing season statistics"""
    analyzer = MCPFirstInningAnalyzer()
    
    print("Using historical season data for predictions...")
    predictions = analyzer.predict_tomorrows_nrfi(season_stats, target_date)
    
    if predictions:
        analyzer.print_nrfi_predictions(predictions)
        
        # Save predictions
        predictions_df = pd.DataFrame(predictions)
        date_str = target_date.replace('/', '') if target_date else "tomorrow"
        filename = f"nrfi_predictions_with_data_{date_str}.csv"
        predictions_df.to_csv(filename, index=False)
        print(f"\nPredictions saved to: {filename}")
        
        return predictions
    
    return []

def get_team_list():
    """Helper function to get list of all MLB teams and their IDs"""
    teams = statsapi.get('teams', {'sportIds': 1, 'activeStatus': 'Yes'})
    team_list = []
    for team in teams['teams']:
        team_list.append({
            'id': team['id'],
            'name': team['name'],
            'abbreviation': team.get('abbreviation', ''),
            'division': team.get('division', {}).get('name', ''),
            'league': team.get('league', {}).get('name', '')
        })
    return team_list

def explore_api():
    """Function to explore the API and understand data structure"""
    print("Exploring MLB Stats API...")
    
    # Get recent schedule
    try:
        recent_games = statsapi.schedule(
            start_date=(datetime.now() - timedelta(days=30)).strftime('%m/%d/%Y'),
            end_date=datetime.now().strftime('%m/%d/%Y')
        )
        
        if recent_games:
            print(f"Found {len(recent_games)} recent games")
            
            # Analyze first completed game
            for game in recent_games:
                if game.get('status') == 'Final':
                    print(f"\nAnalyzing game: {game.get('away_name', 'Unknown')} vs {game.get('home_name', 'Unknown')}")
                    print(f"Game ID: {game['game_id']}")
                    
                    # Get linescore
                    linescore = statsapi.linescore(game['game_id'])
                    print("\nLinescore:")
                    print(linescore)
                    
                    # Get boxscore data
                    boxscore_data = statsapi.boxscore_data(game['game_id'])
                    print(f"\nBoxscore data keys: {list(boxscore_data.keys())}")
                    
                    break
        else:
            print("No recent games found")
            
    except Exception as e:
        print(f"Error exploring API: {e}")

def main():
    """Main function with enhanced options for MCP exploration"""
    analyzer = MCPFirstInningAnalyzer()
    
    # Example: Analyze recent games (last 30 days for better chance of finding games)
    end_date = datetime.now().strftime('%m/%d/%Y')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%m/%d/%Y')
    
    print("Enhanced MLB First Inning Analysis")
    print("="*50)
    print(f"Analyzing games from {start_date} to {end_date}")
    
    try:
        results = analyzer.analyze_period(start_date, end_date, max_games=20)
        
        if results:
            stats = analyzer.calculate_statistics(results)
            analyzer.print_detailed_report(stats, results)
            
            # Show detailed breakdown for first few games
            print(f"\nDetailed Results for First 5 Games:")
            print("-" * 50)
            for i, result in enumerate(results[:5]):
                print(f"Game {i+1}: {result['away_team']} vs {result['home_team']}")
                print(f"  First Inning: {result['away_first_inning_runs']}-{result['home_first_inning_runs']}")
                print(f"  No runs: {result['no_runs_first_inning']}")
                print()
        else:
            print("No completed games found in the specified period.")
            print("This might be during off-season. Let's explore the MCP approach instead.")
            
    except Exception as e:
        print(f"Error in analysis: {e}")
        print("Let's explore alternative approaches...")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_specific_game()
        elif sys.argv[1] == "debug" and len(sys.argv) > 2:
            debug_single_game(sys.argv[2])
        elif sys.argv[1] == "fullday" and len(sys.argv) > 2:
            comprehensive_debug_day(sys.argv[2])
        elif sys.argv[1] == "season" and len(sys.argv) > 3:
            start_date = sys.argv[2]
            end_date = sys.argv[3]
            max_games = int(sys.argv[4]) if len(sys.argv) > 4 else None
            stats, results = analyze_season(start_date, end_date, max_games)
            
            # After season analysis, ask if user wants predictions
            if stats and results:
                user_input = input("\nWould you like to predict tomorrow's NRFI games using this data? (y/n): ")
                if user_input.lower() in ['y', 'yes']:
                    predict_with_season_data(stats)
        elif sys.argv[1] == "predict":
            target_date = sys.argv[2] if len(sys.argv) > 2 else None
            predict_tomorrow(target_date=target_date)
        elif sys.argv[1] == "explore":
            explore_api()
        elif sys.argv[1] == "teams":
            teams = get_team_list()
            print(f"\nFound {len(teams)} MLB teams:")
            for team in teams:
                print(f"ID: {team['id']:3d} | {team['name']:25} | {team['abbreviation']:3} | {team['league']} - {team['division']}")
        else:
            print("Usage:")
            print("  python script.py                              # Run normal analysis")
            print("  python script.py test                         # Test Padres vs Reds game")
            print("  python script.py debug <game_id>              # Debug specific game")
            print("  python script.py fullday <date>               # Debug all games on date")
            print("  python script.py season <start> <end> [max]   # Season analysis")
            print("  python script.py predict [date]               # Predict NRFI for date (default tomorrow)")
            print("  python script.py explore                      # Explore API structure")
            print("  python script.py teams                        # Get team list")
    else:
        main()