module Harness
  class ResqueQueue
    class SendGauge < Job
      @queue = :metrics

      def self.perform(attributes)
        gauge = Gauge.new attributes
        new.log gauge

      rescue EOFError
        attributes[:attempts] ||= 0
        attributes[:attempts] += 1

        if Resque.respond_to?(:enqueue_in) and attributes[:attempts] < 3
          Resque.enqueue_in(10.seconds, Harness::ResqueQueue::SendGauge, attributes)
        else
          raise
        end
      end
    end

    class SendCounter < Job
      @queue = :metrics

      def self.perform(attributes)
        counter = Counter.new attributes
        new.log counter
      end
    end

    def push(measurement)
      if measurement.is_a? Gauge
        Resque.enqueue SendGauge, measurement.attributes
      elsif measurement.is_a? Counter
        Resque.enqueue SendCounter, measurement.attributes
      end
    end
  end
end
