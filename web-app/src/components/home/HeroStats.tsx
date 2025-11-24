'use client';

import { useStats } from '@/hooks/useStats';
import { Container } from '@/components/ui/Container';
import { TrendingUp, DollarSign, Percent } from 'lucide-react';

export function HeroStats() {
    const { stats, loading } = useStats();

    if (loading) {
        return (
            <div className="py-12 bg-black/50 border-b border-white/10 animate-pulse">
                <Container>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                        <div className="h-32 bg-white/5 rounded-xl"></div>
                        <div className="h-32 bg-white/5 rounded-xl"></div>
                        <div className="h-32 bg-white/5 rounded-xl"></div>
                    </div>
                </Container>
            </div>
        );
    }

    if (!stats) return null;

    return (
        <div className="py-12 bg-black/50 border-b border-white/10">
            <Container>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                    <StatCard
                        label="Total Profit"
                        value={`$${stats.total_profit.toLocaleString()}`}
                        icon={<DollarSign className="h-5 w-5 text-success" />}
                        trend="All Time"
                    />
                    <StatCard
                        label="ROI"
                        value={`${stats.roi}%`}
                        icon={<TrendingUp className="h-5 w-5 text-primary" />}
                        trend="Return on Investment"
                    />
                    <StatCard
                        label="Win Rate"
                        value={`${stats.win_rate}%`}
                        icon={<Percent className="h-5 w-5 text-blue-500" />}
                        trend={`${stats.total_bets} Total Bets`}
                    />
                </div>
            </Container>
        </div>
    );
}

function StatCard({ label, value, icon, trend }: { label: string, value: string, icon: React.ReactNode, trend: string }) {
    return (
        <div className="bg-card border border-white/10 rounded-xl p-6 hover:border-primary/50 transition-colors duration-300">
            <div className="flex items-center justify-between mb-4">
                <span className="text-muted-foreground font-medium text-sm">{label}</span>
                <div className="p-2 bg-white/5 rounded-lg">{icon}</div>
            </div>
            <div className="text-3xl font-bold text-foreground mb-1">{value}</div>
            <div className="text-xs text-muted-foreground">{trend}</div>
        </div>
    );
}
