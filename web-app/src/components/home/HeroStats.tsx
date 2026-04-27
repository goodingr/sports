'use client';

import { useState, useEffect } from 'react';
import { useStats } from '@/hooks/useStats';
import { Container } from '@/components/ui/Container';
import { TrendingUp, DollarSign, Percent, ChevronUp, ChevronDown } from 'lucide-react';

export function HeroStats() {
    const { stats, loading, error } = useStats();
    const [isCollapsed, setIsCollapsed] = useState<boolean>(() => {
        if (typeof window === 'undefined') return false;
        return window.localStorage.getItem('statsCollapsed') === 'true';
    });

    // Persist state on change
    useEffect(() => {
        if (typeof window !== 'undefined') {
            window.localStorage.setItem('statsCollapsed', String(isCollapsed));
        }
    }, [isCollapsed]);

    if (loading) {
        return (
            <div className="py-4 bg-black/50 border-b border-white/10 animate-pulse">
                <Container>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-3 max-w-2xl mx-auto">
                        <div className="h-20 bg-white/5 rounded-xl"></div>
                        <div className="h-20 bg-white/5 rounded-xl"></div>
                        <div className="h-20 bg-white/5 rounded-xl"></div>
                    </div>
                </Container>
            </div>
        );
    }

    if (error || !stats) {
        return (
            <div className="py-4 bg-black/50 border-b border-white/10">
                <Container>
                    <div className="max-w-2xl mx-auto text-center text-sm text-muted-foreground py-4">
                        Performance stats are temporarily unavailable.
                    </div>
                </Container>
            </div>
        );
    }

    return (
        <div className={`relative bg-black/50 border-b border-white/10 transition-[padding] duration-300 ease-in-out ${isCollapsed ? 'pb-8' : 'pb-0'}`}>
            <div
                className={`grid transition-[grid-template-rows] duration-300 ease-in-out ${isCollapsed ? 'grid-rows-[0fr]' : 'grid-rows-[1fr]'
                    }`}
            >
                <div className="overflow-hidden">
                    <div className="py-8">
                        <Container>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 max-w-2xl mx-auto">
                                <StatCard
                                    label="Total Profit"
                                    value={`$${stats.total_profit.toLocaleString()}`}
                                    icon={<DollarSign className="h-4 w-4 text-success" />}
                                    trend="All Time"
                                />
                                <StatCard
                                    label="ROI"
                                    value={`${stats.roi}%`}
                                    icon={<TrendingUp className="h-4 w-4 text-primary" />}
                                    trend="Return on Investment"
                                />
                                <StatCard
                                    label="Win Rate"
                                    value={`${stats.win_rate}%`}
                                    icon={<Percent className="h-4 w-4 text-blue-500" />}
                                    trend={`${stats.total_bets} Total Bets`}
                                />
                            </div>
                        </Container>
                    </div>
                </div>
            </div>

            <div className="absolute bottom-0 left-1/2 -translate-x-1/2 translate-y-1/2 z-10 flex flex-col items-center">
                <div
                    className={`mb-2 text-[10px] font-medium text-muted-foreground uppercase tracking-widest transition-opacity duration-300 ${isCollapsed ? 'opacity-100' : 'opacity-0 pointer-events-none'
                        }`}
                >
                    View Stats
                </div>
                <button
                    onClick={() => {
                        const newState = !isCollapsed;
                        setIsCollapsed(newState);
                        if (newState) {
                            window.scrollTo({ top: 0, behavior: 'smooth' });
                        }
                    }}
                    className="flex items-center justify-center w-8 h-8 rounded-full bg-card border border-white/10 text-muted-foreground hover:text-foreground hover:border-primary/50 transition-colors shadow-lg"
                    aria-label={isCollapsed ? "Show stats" : "Hide stats"}
                >
                    {isCollapsed ? <ChevronDown className="h-4 w-4" /> : <ChevronUp className="h-4 w-4" />}
                </button>
            </div>
        </div>
    );
}

function StatCard({ label, value, icon, trend }: { label: string, value: string, icon: React.ReactNode, trend: string }) {
    return (
        <div className="bg-card border border-white/10 p-2 hover:border-primary/50 transition-colors duration-300 flex flex-col items-center justify-center text-center">
            <div className="flex flex-col items-center justify-center mb-1 gap-1">
                <div className="p-1 bg-white/5 rounded-full">{icon}</div>
                <span className="text-muted-foreground font-medium text-[10px] uppercase tracking-wider">{label}</span>
            </div>
            <div className="text-lg font-bold text-foreground mb-0.5">{value}</div>
            <div className="text-[10px] text-muted-foreground">{trend}</div>
        </div>
    );
}
