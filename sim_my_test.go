package main

import (
"fmt"
"math"
"time"
)

func main() {
    anchor := time.Date(2026, 4, 8, 11, 23, 29, 0, time.UTC)
    period := 5 * time.Second
    t1 := anchor.Add(-100 * time.Second).Add(-time.Millisecond).Add(2*time.Second) 
    
    offset := t1.Sub(anchor)
    halfPeriod := period / 2
    var roundedOffset time.Duration
    if offset >= 0 {
roundedOffset = ((offset + halfPeriod) / period) * period
} else {
roundedOffset = ((offset - halfPeriod) / period) * period
}

    fmt.Println(offset, roundedOffset)
}
