##module Ledger

##using DataFrames, CategoricalArrays, CodecZlib, FileIO, TimeZones
using Dates, DataFrames, StatsBase


## convenience functions to simulate c++ map<k,v> += op
function increment!(d::Dict{K,V},k::K,v::V) where V where K
    if haskey(d,k)
        d[k] += v
    else
        d[k] = v
    end
end

## incrementdelete! removes the dangling 0 position key
function incrementdelete!(d::Dict{K,V},k::K,v::V) where V where K
    nv = increment!(d, k, v)
    if nv==zero(V)
        delete!(d, k)
    end
    nv
end

struct Security
is_derivative::Bool
trade_ccy::String
settle_ccy::String
maturity::Union{Missing, Date}
valuation::Function
end

struct Transaction
asofdate::Date
ticker::String
quantity::Float64
price::Float64
end

mutable struct SecurityBalanceSheet
current_position::Float64
avgprice::Float64
mark::Float64
value::Float64
derivatives_offset::Float64
unrealized_pnl::Float64
realized_pnl::Float64
end

Ledger = Dict{String,SecurityBalanceSheet}

function ledger2df(l::Ledger)
    df = [DataFrame(ticker=collect(keys(l))) DataFrame(values(l))]
end

function checkaccounts(l::Ledger)
    df = ledger2df(l)
    ## ignores ccy for now
    assets = sum(df[:value]) + sum(df[:derivatives_offset])
    se = sum(df[:unrealized_pnl]) + sum(df[:realized_pnl])
    assets == se
end

# import Base.+
# function +(a::Position, b::Position)
#     Position(a.value + b.value,
#              a.derivatives_offset + b.derivatives_offset,
#              a.unrealized_pnl + b.unrealized_pnl,
#              a.realized_pnl + b.realized_pnl)
# end


## returns a tuple of unrealized_pnl, realized_pnl
function updateLedger!(ledger::Ledger,t::Transaction,secmaster::Dict{String,Security})
    s = secmaster[t.ticker]
    value = t.quantity * s.valuation(t.price)

    # 3 cases,
    # 1) new position / add to a position
    # 2) reduce position
    # 3) flip position

    # 1) new position
    if !haskey(ledger,t.ticker) || ledger[t.ticker].current_position == 0
        if s.is_derivative
            ledger[t.ticker] = SecurityBalanceSheet(t.quantity,t.price,t.price,value,-value,0,0)
        else
            ledger[t.ticker] = SecurityBalanceSheet(t.quantity,t.price,t.price,value,0,0,0)
            ## debit cash
            if haskey(ledger,s.settle_ccy)
                ledger[s.settle_ccy].current_position -= value
                ledger[s.settle_ccy].value -= value
            else
                ledger[s.settle_ccy] = SecurityBalanceSheet(-value,1,1,-value,0,0,0)
            end
        end
        ## no SE impact
        return (0.,0.)
    else
        ## reference to secbs
        secbs = ledger[t.ticker]
        ## add to position
        if sign(secbs.current_position) == sign(t.quantity)
            old_pos = secbs.current_position
            new_pos = old_pos + t.quantity
            secbs.current_position = new_pos
            secbs.avgprice = secbs.avgprice * old_pos / new_pos + t.price * t.quantity / new_pos
            ## secbs.mark -- no change
            secbs.value += value
            if s.is_derivative
                secbs.derivatives_offset += -value
            else
                ## if we are adding to a position, the settle_ccy should already exist in the hash
                ledger[s.settle_ccy].current_position -= value
                ledger[s.settle_ccy].value -= value
            end
            ## no SE impact
            return (0.,0.)
        else
            ## closeout
            if -t.quantity == secbs.current_position
                secbs.realized_pnl += -value - t.quantity * s.valuation(secbs.avgprice)
                if !s.is_derivative
                    ledger[s.settle_ccy].current_position -= value
                    ledger[s.settle_ccy].value -= value
                end
                ## zero out position
                secbs.current_position = 0
                secbs.avgprice = NaN
                secbs.mark = NaN
                secbs.value = 0
                secbs.derivatives_offset = 0
                secbs.unrealized_pnl = 0
                return (secbs.unrealized_pnl,secbs.realized_pnl)
            else
                ## reduction
                proportion = -t.quantity / secbs.current_position
                error("not implemented")
            end
        end
    end
end
# function run_pnl(transactions::Vector{Transaction},secmaster::Dict{String,Security},prices::NDSparse)

#     ## add checks
#     ## 1) all secmasters defined for unique(transaction.ticker)
#     ## 2) all prices available for position/date in prices    
    
#     # show(transactions)
#     # println()
#     # show(secmaster)
#     # println()
#     # show(prices)
#     # println()
#     ##return pricesix[Date(2019,1,2),"AMZN"].price

#     trade_dates = [x.asofdate for x in transactions];
#     start_date = minimum(trade_dates)
#     end_date = maximum(trade_dates)

#     ## run pnl for all weekdays in range
#     pnl_dates = filter(d -> dayofweek(d) != Dates.Saturday && dayofweek(d) != Dates.Sunday, start_date:Dates.Day(1):end_date)

#     holdings = Dict{String,Float64}()
#     derivatives_settlement: = Dict{String,Float64}()

#     transaction_idx = 1
#     ## loop dates, permute ledger for each date
#     ## run pnl for that day when next trade date is encountered
#     for pnl_date in pnl_dates
#         println("working on:",pnl_date)
#         while (transactions[transaction_idx].asofdate == pnl_date)
#             ticker = transactions[transaction_idx].ticker
#             updateLedgers(ticker,secmaster[ticker],holdings,derivatives_settlement)
#             transaction_idx += 1
#         end
#     end
# end



secmaster = Dict{String,Security}("AMZN" => Security(false,"USD","USD",missing,p -> p),
                                  "IBM" => Security(false,"USD","USD",missing,p -> p)
                                  )


## sec prices
ibm=DataFrame(ticker="IBM",asofdate=[Date(2019,1,2),Date(2019,1,3)],price=[100,101])
amzn=DataFrame(ticker="AMZN",asofdate=[Date(2019,1,2),Date(2019,1,3)],price=[200,201])
prices = [ibm;amzn]

trades = [Transaction(Date(2019,1,2),"AMZN",1000,200),
          Transaction(Date(2019,1,20,),"IBM",10,100),
          Transaction(Date(2019,1,20,),"IBM",-10,100.50)
          ]



l = Ledger()

@assert updateLedger!(l,Transaction(Date(2019,1,2),"AMZN",1000,200),secmaster)==(0.,0.)
@assert l["AMZN"].current_position==1000
@assert l["USD"].current_position==-1000*200
@assert checkaccounts(l)
@assert updateLedger!(l,Transaction(Date(2019,1,2),"AMZN",2000,200),secmaster)==(0.,0.)
@assert l["AMZN"].current_position==3000
@assert l["USD"].current_position==(-1000*200 + -2000*200)
@assert checkaccounts(l)

@assert updateLedger!(l,Transaction(Date(2019,1,2),"IBM",1000,101),secmaster)==(0.,0.)
@assert l["IBM"].current_position==1000
@assert l["USD"].current_position==(-1000*200 + -2000*200 + -1000*101)
@assert checkaccounts(l)
@assert updateLedger!(l,Transaction(Date(2019,1,2),"AMZN",-3000,200),secmaster)==(0.,0.)
@assert checkaccounts(l)


l2 = Ledger()
@assert updateLedger!(l2,Transaction(Date(2019,1,2),"AMZN",1000,200),secmaster)==(0.,0.)
@assert updateLedger!(l2,Transaction(Date(2019,1,2),"AMZN",-1000,201),secmaster)==(0.,1000.)

##[DataFrame(ticker=collect(keys(l.positions))) DataFrame(values(l.positions))]
##@assert countmap(l.positions)
##@assert values(l.positions)

##end # module Ledger
