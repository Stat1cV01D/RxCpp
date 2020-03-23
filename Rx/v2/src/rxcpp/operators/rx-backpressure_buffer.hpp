// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.txt in the project root for license information.
// Modifications by stat1c_v01d

#pragma once

/*! \file rx-backpressure_buffer.hpp

    \brief Return an observable that emits buffers every period time interval and collects items from this observable for period of time into each produced buffer.
           If the skip parameter is set, Return an observable that emits buffers every skip time interval and collects items from this observable for period of time into each produced buffer, on the specified scheduler.

    \tparam Duration      the type of the time interval
    \tparam Coordination  the type of the scheduler (optional).

    \param period        the period of time each buffer collects items before it is emitted.
    \param skip          the period of time after which a new buffer will be created (optional).
    \param coordination  the scheduler for the buffers (optional).

    \return  Observable that emits buffers every period time interval and collect items from this observable for period of time into each produced buffer.
             If the skip parameter is set, return an Observable that emits buffers every skip time interval and collect items from this observable for period of time into each produced buffer.
*/

#if !defined(RXCPP_OPERATORS_RX_BACKPRESSURE_BUFFER_HPP)
#define RXCPP_OPERATORS_RX_BACKPRESSURE_BUFFER_HPP

#include "../rx-includes.hpp"

namespace rxcpp {

namespace operators {

namespace detail {

struct backpressure_strategy
{
    enum type {
        error,       // throw error
        drop_oldest, // drop the oldest value in buffer
        drop_latest, // drop the latest value in buffer
        custom       // custom action
    };
};

template<class... AN>
struct backpressure_buffer_invalid_arguments {};

template<class... AN>
struct backpressure_buffer_invalid : public rxo::operator_base<backpressure_buffer_invalid_arguments<AN...>> {
    using type = observable<backpressure_buffer_invalid_arguments<AN...>, backpressure_buffer_invalid<AN...>>;
};
template<class... AN>
using backpressure_buffer_invalid_t = typename backpressure_buffer_invalid<AN...>::type;

template<class T, class Coordination>
struct backpressure_buffer
{
    typedef rxu::decay_t<T> value_type;
    typedef rxu::decay_t<Coordination> coordination_type;
    typedef typename coordination_type::coordinator_type coordinator_type;
    typedef std::deque<value_type> actions_deque;
    typedef std::function<actions_deque(actions_deque)> action_func;

    struct backpressure_buffer_values
    {
        backpressure_buffer_values(backpressure_strategy::type s, int sz, coordination_type c, action_func a)
            : strategy(s)
            , size(sz)
            , coordination(c)
            , action(a)
        {
        }
        backpressure_strategy::type strategy;
        int size;
        coordination_type coordination;
        action_func action;
    };
    backpressure_buffer_values initial;

    backpressure_buffer(backpressure_strategy::type strategy, int size, coordination_type coordination, action_func action)
        : initial(strategy, size, coordination, std::move(action))
    {
    }

    template<class Subscriber>
    struct backpressure_buffer_observer
    {
        typedef backpressure_buffer_observer<Subscriber> this_type;
        typedef T value_type;
        typedef rxu::decay_t<Subscriber> dest_type;
        typedef observer<value_type, this_type> observer_type;

        struct backpressure_buffer_subscriber_values : public backpressure_buffer_values
        {
            backpressure_buffer_subscriber_values(composite_subscription cs, dest_type d, backpressure_buffer_values v, coordinator_type c)
                : backpressure_buffer_values(v)
                , cs(std::move(cs))
                , dest(std::move(d))
                , coordinator(std::move(c))
                , worker(coordinator.get_worker())
                , expected(worker.now())
            {
            }
            composite_subscription cs;
            dest_type dest;
            coordinator_type coordinator;
            rxsc::worker worker;
            mutable actions_deque items;
            rxsc::scheduler::clock_type::time_point expected;
        };
        std::shared_ptr<backpressure_buffer_subscriber_values> state;
        std::shared_ptr<std::mutex> lock;

        backpressure_buffer_observer(composite_subscription cs, dest_type d, backpressure_buffer_values v, coordinator_type c, std::shared_ptr<std::mutex> m)
            : state(std::make_shared<backpressure_buffer_subscriber_values>(backpressure_buffer_subscriber_values(std::move(cs), std::move(d), v, std::move(c))))
            , lock(std::move(m))
        {
            auto localState = state;            

            auto disposer = [=](const rxsc::schedulable&){
                localState->cs.unsubscribe();
                localState->dest.unsubscribe();
                localState->worker.unsubscribe();
            };
            auto selectedDisposer = on_exception(
                [&](){return localState->coordinator.act(disposer);},
                localState->dest);
            if (selectedDisposer.empty()) {
                return;
            }

            localState->dest.add([=](){
                localState->worker.schedule(selectedDisposer.get());
            });
            localState->cs.add([=](){
                localState->worker.schedule(selectedDisposer.get());
            });
        }
        void on_next(T v) const {
            auto localState = state;
            auto localMutex = lock;
            auto work = [localMutex, localState](const rxsc::schedulable&){
                T value;
                {
                    std::unique_lock<std::mutex> m(*localMutex);
                    if (localState->items.empty())
                        return;
                    value = std::move(localState->items.front());
                    localState->items.pop_front();
                }
                localState->dest.on_next(value);
            };
            auto selectedWork = on_exception(
                [&](){return localState->coordinator.act(work);},
                localState->dest);
            if (selectedWork.empty()) {
                return;
            }

            {
                std::unique_lock<std::mutex> m(*localMutex);
                if (localState->items.size() >= localState->size) {
                    switch (localState->strategy) {
                    case backpressure_strategy::error:
                        rxu::throw_exception("backpressure_buffer() had buffer overrun.");
                        break;
                    case backpressure_strategy::drop_latest:
                        localState->items.pop_back();
                        break;
                    case backpressure_strategy::drop_oldest:
                        localState->items.pop_front();
                        break;
                    case backpressure_strategy::custom:
                        localState->items = localState->action(localState->items);
                        break;
                    }
                }
                localState->items.push_back(v);
            }
            localState->worker.schedule(selectedWork.get());
        }
        void on_error(rxu::error_ptr e) const {
            auto localState = state;
            auto work = [e, localState](const rxsc::schedulable&){
                localState->dest.on_error(e);
            };
            auto selectedWork = on_exception(
                [&](){return localState->coordinator.act(work);},
                localState->dest);
            if (selectedWork.empty()) {
                return;
            }
            localState->worker.schedule(selectedWork.get());
        }
        void on_completed() const {
            auto localState = state;
            auto work = [localState](const rxsc::schedulable&){
                on_exception(
                    [&](){
                        while (!localState->items.empty()) {
                            localState->dest.on_next(std::move(localState->items.front()));
                            localState->items.pop_front();
                        }
                        return true;
                    },
                    localState->dest);
                localState->dest.on_completed();
            };
            auto selectedWork = on_exception(
                [&](){return localState->coordinator.act(work);},
                localState->dest);
            if (selectedWork.empty()) {
                return;
            }
            localState->worker.schedule(selectedWork.get());
        }

        static subscriber<T, observer<T, this_type>> make(dest_type d, backpressure_buffer_values v) {
            auto cs = composite_subscription();
            auto coordinator = v.coordination.create_coordinator();
            std::shared_ptr<std::mutex> lock = std::make_shared<std::mutex>();

            return make_subscriber<T>(cs, this_type(cs, std::move(d), std::move(v), std::move(coordinator), std::move(lock)));
        }
    };

    template<class Subscriber>
    auto operator()(Subscriber dest) const
        -> decltype(backpressure_buffer_observer<Subscriber>::make(std::move(dest), initial)) {
        return      backpressure_buffer_observer<Subscriber>::make(std::move(dest), initial);
    }
};

}

/*! @copydoc rx-backpressure_buffer.hpp
*/
template<class... AN>
auto backpressure_buffer(AN&&... an)
    ->      operator_factory<backpressure_buffer_tag, AN...> {
     return operator_factory<backpressure_buffer_tag, AN...>(std::make_tuple(std::forward<AN>(an)...));
}

}

template<>
struct member_overload<backpressure_buffer_tag>
{
    template<class Observable,
        class Enabled = rxu::enable_if_all_true_type_t<
            is_observable<Observable>>,
        class SourceValue = rxu::value_type_t<Observable>,
        class BackpressureStrategy = rxo::detail::backpressure_strategy::type,
        class BackpressureBuffer = rxo::detail::backpressure_buffer<SourceValue, serialize_one_worker>,
        class Value = rxu::value_type_t<BackpressureBuffer>>
    static auto member(Observable&& o)
        -> decltype(o.template lift<Value>(BackpressureBuffer(BackpressureStrategy::error, 2, serialize_new_thread(), nullptr))) {
        return      o.template lift<Value>(BackpressureBuffer(BackpressureStrategy::error, 2, serialize_new_thread(), nullptr));
    }

    template<class Observable, class Coordination,
        class Enabled = rxu::enable_if_all_true_type_t<
            is_observable<Observable>,
            is_coordination<Coordination>>,
        class SourceValue = rxu::value_type_t<Observable>,
        class BackpressureStrategy = rxo::detail::backpressure_strategy::type,
        class BackpressureBuffer = rxo::detail::backpressure_buffer<SourceValue, rxu::decay_t<Coordination>>,
        class Value = rxu::value_type_t<BackpressureBuffer>>
    static auto member(Observable&& o, BackpressureStrategy &&strategy, Coordination&& cn)
        -> decltype(o.template lift<Value>(BackpressureBuffer(std::forward<BackpressureStrategy>(strategy), 2, std::forward<Coordination>(cn), nullptr))) {
        return      o.template lift<Value>(BackpressureBuffer(std::forward<BackpressureStrategy>(strategy), 2, std::forward<Coordination>(cn), nullptr));
    }

    template<class Observable,
        class Enabled = rxu::enable_if_all_true_type_t<
            is_observable<Observable>>,
        class SourceValue = rxu::value_type_t<Observable>,
        class BackpressureStrategy = rxo::detail::backpressure_strategy::type,
        class BackpressureBuffer = rxo::detail::backpressure_buffer<SourceValue, serialize_one_worker>,
        typename BackpressureAction = typename BackpressureBuffer::action_func,
        class Value = rxu::value_type_t<BackpressureBuffer>>
    static auto member(Observable&& o, int size, const BackpressureAction &action)
        -> decltype(o.template lift<Value>(BackpressureBuffer(BackpressureStrategy::custom, size, serialize_new_thread(), action))) {
        return      o.template lift<Value>(BackpressureBuffer(BackpressureStrategy::custom, size, serialize_new_thread(), action));
    }

    template<class Observable, class Coordination,
        class Enabled = rxu::enable_if_all_true_type_t<
            is_observable<Observable>,
            is_coordination<Coordination>>,
        class SourceValue = rxu::value_type_t<Observable>,
        class BackpressureStrategy = rxo::detail::backpressure_strategy::type,
        class BackpressureBuffer = rxo::detail::backpressure_buffer<SourceValue, rxu::decay_t<Coordination>>,
        class Value = rxu::value_type_t<BackpressureBuffer>>
    static auto member(Observable&& o, int size, Coordination&& cn)
        -> decltype(o.template lift<Value>(BackpressureBuffer(BackpressureStrategy::drop_oldest, size, std::forward<Coordination>(cn), nullptr))) {
        return      o.template lift<Value>(BackpressureBuffer(BackpressureStrategy::drop_oldest, size, std::forward<Coordination>(cn), nullptr));
    }

    template<class Observable, class Coordination,
        class Enabled = rxu::enable_if_all_true_type_t<
            is_observable<Observable>,
            is_coordination<Coordination>>,
        class SourceValue = rxu::value_type_t<Observable>,
        class BackpressureStrategy = rxo::detail::backpressure_strategy::type,
        class BackpressureBuffer = rxo::detail::backpressure_buffer<SourceValue, rxu::decay_t<Coordination>>,
        class Value = rxu::value_type_t<BackpressureBuffer>>
    static auto member(Observable&& o, BackpressureStrategy &&strategy, int size, Coordination&& cn)
        -> decltype(o.template lift<Value>(BackpressureBuffer(std::forward<BackpressureStrategy>(strategy), size, std::forward<Coordination>(cn), nullptr))) {
        return      o.template lift<Value>(BackpressureBuffer(std::forward<BackpressureStrategy>(strategy), size, std::forward<Coordination>(cn), nullptr));
    }

    template<class... AN>
    static operators::detail::backpressure_buffer_invalid_t<AN...> member(AN...) {
        std::terminate();
        return {};
        static_assert(sizeof...(AN) == 10000, "backpressure_buffer takes (optional Strategy, optional BufferSize, optional Coordination)");
    }
};

}

#endif
