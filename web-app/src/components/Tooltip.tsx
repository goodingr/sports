'use client'

import { useState, useRef, useEffect } from 'react'
import Link from 'next/link'

interface TooltipProps {
    children: React.ReactNode
    content: React.ReactNode
    href?: string
}

export default function Tooltip({ children, content, href }: TooltipProps) {
    const [isVisible, setIsVisible] = useState(false)
    const [position, setPosition] = useState<'top' | 'bottom'>('bottom')
    const triggerRef = useRef<HTMLSpanElement>(null)
    const tooltipRef = useRef<HTMLDivElement>(null)

    useEffect(() => {
        if (isVisible && triggerRef.current && tooltipRef.current) {
            const triggerRect = triggerRef.current.getBoundingClientRect()
            const tooltipRect = tooltipRef.current.getBoundingClientRect()
            const spaceBelow = window.innerHeight - triggerRect.bottom
            const spaceAbove = triggerRect.top

            // Show above if not enough space below
            if (spaceBelow < tooltipRect.height + 10 && spaceAbove > tooltipRect.height + 10) {
                setPosition('top')
            } else {
                setPosition('bottom')
            }
        }
    }, [isVisible])

    const tooltipContent = (
        <div
            ref={tooltipRef}
            className={`absolute z-50 px-3 py-2 text-sm bg-gray-900 dark:bg-gray-800 text-white rounded-lg shadow-lg 
        transition-opacity duration-200 ${isVisible ? 'opacity-100' : 'opacity-0 pointer-events-none'}
        ${position === 'top' ? 'bottom-full mb-2' : 'top-full mt-2'}
        left-1/2 transform -translate-x-1/2 whitespace-nowrap max-w-xs`}
            style={{ minWidth: '150px' }}
        >
            {content}
            {href && (
                <Link
                    href={href}
                    className="block mt-2 text-blue-400 hover:text-blue-300 font-medium"
                    onClick={() => setIsVisible(false)}
                >
                    Learn more →
                </Link>
            )}
            <div
                className={`absolute left-1/2 transform -translate-x-1/2 w-2 h-2 bg-gray-900 dark:bg-gray-800 rotate-45
          ${position === 'top' ? 'bottom-0 translate-y-1' : 'top-0 -translate-y-1'}`}
            />
        </div>
    )

    return (
        <span className="relative inline-block">
            <span
                ref={triggerRef}
                className="inline-flex items-center gap-1 cursor-help border-b border-dotted border-gray-400 dark:border-gray-600"
                onMouseEnter={() => setIsVisible(true)}
                onMouseLeave={() => setIsVisible(false)}
                onFocus={() => setIsVisible(true)}
                onBlur={() => setIsVisible(false)}
                tabIndex={0}
            >
                {children}
                <svg
                    className="w-4 h-4 text-gray-500 dark:text-gray-400"
                    fill="none"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="2"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                >
                    <path d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
            </span>
            {tooltipContent}
        </span>
    )
}
