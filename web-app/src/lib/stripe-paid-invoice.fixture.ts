import type Stripe from "stripe";

type Subscription = Pick<Stripe.Subscription, "id" | "customer" | "livemode">;
type Item = { id: string; price: { id: string }; current_period_end: number };

export function paidInvoiceFixture(
    sub: Subscription, item: Item, start = 1_770_000_000, end = item.current_period_end,
) {
    const id = `in_${sub.id}_${end}`;
    return {
        id,
        customer: typeof sub.customer === "string" ? sub.customer : sub.customer.id,
        livemode: sub.livemode,
        status: "paid",
        currency: "usd",
        amount_paid: 2900,
        status_transitions: { paid_at: start },
        parent: { type: "subscription_details", subscription_details: { subscription: sub.id } },
        lines: {
            has_more: false,
            data: [{
                id: `il_${id}`,
                invoice: id,
                subscription: sub.id,
                livemode: sub.livemode,
                amount: 2900,
                discount_amounts: [] as { amount: number; discount: string }[],
                currency: "usd",
                pricing: { type: "price_details", price_details: { price: item.price.id } },
                parent: {
                    type: "subscription_item_details",
                    subscription_item_details: { subscription: sub.id, subscription_item: item.id, proration: false },
                },
                period: { start, end },
            }],
        },
    };
}
