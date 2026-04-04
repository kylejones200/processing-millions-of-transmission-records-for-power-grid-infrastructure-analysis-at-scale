"""
Power Grid Infrastructure Analysis at Scale
Processing millions of transmission records for grid reliability and capacity planning

NOTE: Uses synthetic data for demonstration (HIFLD_Transmission_Lines.parquet not found).
For production, download the HIFLD dataset from:
https://hifld-geoplatform.opendata.arcgis.com/
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

np.random.seed(42)
plt.rcParams.update({'font.family': 'serif','axes.spines.top': False,'axes.spines.right': False,'axes.linewidth': 0.8})

def save_fig(path: str):
    plt.tight_layout(); plt.savefig(path, bbox_inches='tight'); plt.close()

@dataclass
class Config:
    data_path: str = "HIFLD_Transmission_Lines.parquet"
    voltage_capacity_map: Dict[int, int] = None
    
    def __post_init__(self):
        if self.voltage_capacity_map is None:
            self.voltage_capacity_map = {
                765: 2400, 500: 1200, 345: 600, 230: 300, 138: 150
            }

def generate_synthetic_grid_data(n_lines: int = 50000) -> pd.DataFrame:
    """Generate synthetic transmission line data for demonstration"""
    np.random.seed(42)
    
    voltage_classes = [(765, '765'), (500, '500'), (345, '345-765'), (230, '220-287'), (138, '100-139')]
    owners = ['PJM', 'MISO', 'SPP', 'ERCOT', 'CAISO', 'Duke Energy', 'Southern Company', 
              'Exelon', 'NextEra Energy', 'American Electric Power', 'Dominion Energy',
              'Berkshire Hathaway Energy', 'Entergy', 'FirstEnergy', 'Xcel Energy']
    
    states = ['TX', 'CA', 'PA', 'IL', 'OH', 'NC', 'GA', 'FL', 'NY', 'MI']
    statuses = ['IN SERVICE'] * 90 + ['PROPOSED'] * 8 + ['DECOMMISSIONED'] * 2
    types = ['OVERHEAD'] * 95 + ['UNDERGROUND'] * 5
    
    data = {
        'ID': np.arange(1, n_lines + 1),
        'VOLTAGE': np.random.choice([v[0] for v in voltage_classes], n_lines, p=[0.02, 0.08, 0.35, 0.40, 0.15]),
        'VOLT_CLASS': '',
        'OWNER': np.random.choice(owners, n_lines),
        'STATE': np.random.choice(states, n_lines),
        'STATUS': np.random.choice(statuses, n_lines),
        'TYPE': np.random.choice(types, n_lines),
        'START_LAT': np.random.uniform(25, 49, n_lines),
        'START_LON': np.random.uniform(-125, -67, n_lines),
        'END_LAT': np.random.uniform(25, 49, n_lines),
        'END_LON': np.random.uniform(-125, -67, n_lines),
        'SHAPE__Length': np.random.exponential(50000, n_lines),
        'SUB_1': [f'SUB_{np.random.randint(1, 1000)}' for _ in range(n_lines)],
        'SUB_2': [f'SUB_{np.random.randint(1, 1000)}' for _ in range(n_lines)]
    }
    
    df = pd.DataFrame(data)
    
    # Map voltage to voltage class
    voltage_map = {v[0]: v[1] for v in voltage_classes}
    df['VOLT_CLASS'] = df['VOLTAGE'].map(voltage_map)
    
    return df

class TransmissionLinesService:
    """High-performance service for transmission line data analysis"""
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.data = self._load_data()
    
    def _load_data(self) -> pd.DataFrame:
        """Load transmission lines from Parquet/CSV or generate synthetic"""
        p = Path(self.cfg.data_path)
        
        if p.exists():
            if p.suffix == '.parquet':
                return pd.read_parquet(p)
            return pd.read_csv(p)
        
        print(f"NOTE: {self.cfg.data_path} not found, using synthetic data\n")
        return generate_synthetic_grid_data()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Calculate comprehensive grid statistics"""
        df = self.data
        
        return {
            'total_lines': len(df),
            'in_service': len(df[df['STATUS'] == 'IN SERVICE']),
            'total_miles': df['SHAPE__Length'].sum() / 5280,
            'avg_voltage': df['VOLTAGE'].mean(),
            'unique_owners': df['OWNER'].nunique(),
            'states_covered': df['STATE'].nunique(),
            'overhead_pct': (df['TYPE'].str.contains('OVERHEAD', na=False).sum() / len(df)) * 100,
            'voltage_classes': df['VOLT_CLASS'].nunique()
        }
    
    def get_voltage_distribution(self) -> pd.DataFrame:
        """Voltage class breakdown with counts and percentages"""
        vc = self.data['VOLT_CLASS'].value_counts()
        return pd.DataFrame({
            'voltage_class': vc.index,
            'count': vc.values,
            'percentage': (vc.values / len(self.data) * 100).round(2)
        })
    
    def analyze_voltage_hierarchy(self) -> Dict[str, Dict[str, float]]:
        """Analyze grid by voltage hierarchy"""
        df = self.data
        
        categories = {
            'Ultra-High (>=500 kV)': df[df['VOLTAGE'] >= 500],
            'Extra-High (345-499 kV)': df[(df['VOLTAGE'] >= 345) & (df['VOLTAGE'] < 500)],
            'High (220-344 kV)': df[(df['VOLTAGE'] >= 220) & (df['VOLTAGE'] < 345)],
            'Sub-Transmission (100-219 kV)': df[(df['VOLTAGE'] >= 100) & (df['VOLTAGE'] < 220)]
        }
        
        result = {}
        for name, lines in categories.items():
            if len(lines) > 0:
                result[name] = {
                    'count': len(lines),
                    'percentage': round(len(lines) / len(df) * 100, 2),
                    'miles': round(lines['SHAPE__Length'].sum() / 5280, 2),
                    'avg_voltage': round(lines['VOLTAGE'].mean(), 1),
                    'owners': lines['OWNER'].nunique()
                }
        
        return result
    
    def get_major_utilities(self, top_n: int = 15) -> pd.DataFrame:
        """Major utilities ranked by transmission line count"""
        utility_stats = self.data.groupby('OWNER').agg({
            'ID': 'count',
            'SHAPE__Length': 'sum',
            'VOLTAGE': 'mean',
            'VOLT_CLASS': 'nunique'
        }).reset_index()
        
        utility_stats.columns = ['owner', 'lines', 'total_length', 'avg_voltage', 'voltage_classes']
        utility_stats['miles'] = (utility_stats['total_length'] / 5280).round(0)
        utility_stats = utility_stats.nlargest(top_n, 'lines')
        
        return utility_stats[['owner', 'lines', 'miles', 'avg_voltage', 'voltage_classes']]
    
    def identify_critical_corridors(self, min_voltage: int = 345, top_n: int = 20) -> pd.DataFrame:
        """Identify critical transmission corridors"""
        df = self.data[self.data['VOLTAGE'] >= min_voltage].copy()
        
        corridors = df.groupby(['SUB_1', 'SUB_2']).agg({
            'ID': 'count',
            'VOLTAGE': ['mean', 'max'],
            'SHAPE__Length': 'sum'
        }).reset_index()
        
        corridors.columns = ['sub_1', 'sub_2', 'parallel_lines', 'avg_voltage', 'max_voltage', 'total_length']
        
        # Criticality score: voltage (40%) + redundancy (40%) + length (20%)
        corridors['criticality'] = (
            (corridors['max_voltage'] / 765) * 40 +
            (corridors['parallel_lines'] / corridors['parallel_lines'].max()) * 40 +
            (corridors['total_length'] / corridors['total_length'].max()) * 20
        )
        
        return corridors.nlargest(top_n, 'criticality')
    
    def analyze_capacity(self, forecast_mw: float, corridor_ids: List[str]) -> Dict[str, Any]:
        """Analyze corridor capacity vs forecast load"""
        df = self.data
        corridor_lines = df[df['SUB_1'].isin(corridor_ids) | df['SUB_2'].isin(corridor_ids)]
        
        total_capacity = 0
        for _, line in corridor_lines.iterrows():
            voltage = line['VOLTAGE']
            closest_v = min(self.cfg.voltage_capacity_map.keys(), key=lambda x: abs(x - voltage))
            total_capacity += self.cfg.voltage_capacity_map[closest_v]
        
        utilization = (forecast_mw / total_capacity * 100) if total_capacity > 0 else 0
        
        return {
            'forecast_mw': forecast_mw,
            'capacity_mw': total_capacity,
            'utilization_pct': round(utilization, 2),
            'adequate': utilization < 80,
            'corridor_count': len(corridor_lines),
            'avg_voltage': round(corridor_lines['VOLTAGE'].mean(), 1)
        }

def plot_voltage_distribution(service: TransmissionLinesService):
    """Visualize voltage class distribution"""
    dist = service.get_voltage_distribution()
    
    plt.figure(figsize=(10, 6))
    plt.barh(dist['voltage_class'], dist['count'], color='steelblue')
    plt.xlabel('Number of Transmission Lines')
    plt.ylabel('Voltage Class')
    plt.title('US Transmission Grid: Voltage Class Distribution')
    
    for i, (count, pct) in enumerate(zip(dist['count'], dist['percentage'])):
        plt.text(count + max(dist['count'])*0.01, i, f'{count:,} ({pct:.1f}%)', va='center')
    
    save_fig('grid_voltage_distribution.png')

def plot_utility_territories(service: TransmissionLinesService):
    """Visualize major utility territories"""
    utilities = service.get_major_utilities(top_n=10)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Line counts
    ax1.barh(utilities['owner'], utilities['lines'], color='darkorange')
    ax1.set_xlabel('Number of Lines')
    ax1.set_title('Top 10 Utilities by Line Count')
    ax1.invert_yaxis()
    
    # Network miles
    ax2.barh(utilities['owner'], utilities['miles'], color='forestgreen')
    ax2.set_xlabel('Network Miles')
    ax2.set_title('Top 10 Utilities by Network Length')
    ax2.invert_yaxis()
    
    save_fig('grid_utility_territories.png')

def plot_critical_corridors(service: TransmissionLinesService):
    """Visualize critical transmission corridors"""
    corridors = service.identify_critical_corridors(top_n=15)
    
    plt.figure(figsize=(12, 8))
    
    # Bubble chart: parallel lines vs voltage, sized by criticality
    plt.scatter(corridors['max_voltage'], corridors['parallel_lines'], 
                s=corridors['criticality']*10, alpha=0.6, c=corridors['criticality'],
                cmap='Reds', edgecolors='black', linewidth=0.5)
    
    plt.xlabel('Maximum Voltage (kV)')
    plt.ylabel('Number of Parallel Lines')
    plt.title('Critical Transmission Corridors\n(bubble size = criticality score)')
    plt.colorbar(label='Criticality Score')
    
    save_fig('grid_critical_corridors.png')

def plot_hierarchy_breakdown(hierarchy: Dict[str, Dict[str, float]]):
    """Visualize voltage hierarchy breakdown"""
    categories = list(hierarchy.keys())
    counts = [h['count'] for h in hierarchy.values()]
    miles = [h['miles'] for h in hierarchy.values()]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    colors = ['#8B0000', '#FF4500', '#FFA500', '#FFD700']
    
    # Line counts
    ax1.pie(counts, labels=categories, autopct='%1.1f%%', colors=colors, startangle=90)
    ax1.set_title('Transmission Lines by Voltage Class')
    
    # Network miles
    ax2.pie(miles, labels=categories, autopct='%1.0f mi', colors=colors, startangle=90)
    ax2.set_title('Network Length by Voltage Class')
    
    save_fig('grid_voltage_hierarchy.png')

def main():
    cfg = Config()
    service = TransmissionLinesService(cfg)
    
    # Grid statistics
    stats = service.get_statistics()
    print(f"US Transmission Grid Statistics")
    print(f"{'='*60}")
    print(f"Total lines:      {stats['total_lines']:>10,}")
    print(f"In service:       {stats['in_service']:>10,} ({stats['in_service']/stats['total_lines']*100:.1f}%)")
    print(f"Network length:   {stats['total_miles']:>10,.0f} miles")
    print(f"Average voltage:  {stats['avg_voltage']:>10,.0f} kV")
    print(f"Unique owners:    {stats['unique_owners']:>10,}")
    print(f"States covered:   {stats['states_covered']:>10,}")
    print(f"Overhead lines:   {stats['overhead_pct']:>10,.1f}%")
    
    # Voltage hierarchy
    print(f"\nVoltage Hierarchy Analysis")
    print(f"{'='*60}")
    hierarchy = service.analyze_voltage_hierarchy()
    for category, metrics in hierarchy.items():
        print(f"\n{category}")
        print(f"  Lines: {metrics['count']:,} ({metrics['percentage']:.1f}%)")
        print(f"  Miles: {metrics['miles']:,.0f}")
        print(f"  Avg voltage: {metrics['avg_voltage']:.0f} kV")
        print(f"  Owners: {metrics['owners']}")
    
    # Major utilities
    print(f"\nTop 10 Transmission Owners")
    print(f"{'='*60}")
    utilities = service.get_major_utilities(10)
    for _, row in utilities.iterrows():
        print(f"{row['owner']:<25} {row['lines']:>6,} lines  {row['miles']:>8,.0f} mi  {row['avg_voltage']:>6.0f} kV")
    
    # Critical corridors
    print(f"\nTop 10 Critical Transmission Corridors")
    print(f"{'='*60}")
    corridors = service.identify_critical_corridors(top_n=10)
    for _, c in corridors.iterrows():
        print(f"{c['sub_1']:<12} - {c['sub_2']:<12} | {c['parallel_lines']:>2} lines | {c['max_voltage']:>3.0f} kV | Score: {c['criticality']:>4.1f}")
    
    # Capacity analysis
    print(f"\nTransmission Capacity Analysis")
    print(f"{'='*60}")
    sample_corridors = corridors['sub_1'].head(5).tolist()
    forecast_mw = 15000
    capacity = service.analyze_capacity(forecast_mw, sample_corridors)
    print(f"Forecast load:    {capacity['forecast_mw']:>10,} MW")
    print(f"Corridor capacity:{capacity['capacity_mw']:>10,} MW")
    print(f"Utilization:      {capacity['utilization_pct']:>10.1f}%")
    print(f"Adequate:         {capacity['adequate']}")
    print(f"Corridors:        {capacity['corridor_count']:>10,} lines")
    print(f"Avg voltage:      {capacity['avg_voltage']:>10.1f} kV")
    
    # Generate visualizations
    plot_voltage_distribution(service)
    plot_utility_territories(service)
    plot_critical_corridors(service)
    plot_hierarchy_breakdown(hierarchy)
    
    print(f"\nOutputs: grid_voltage_distribution.png, grid_utility_territories.png, grid_critical_corridors.png, grid_voltage_hierarchy.png\n")

if __name__ == "__main__":
    main()

