# Loop through each JMX file in the current directory
for jmx_file in ./*.jmx; do
    # Extract the test name from the JMX file (assumes JMX files are named consistently)
    test_name=$(basename "${jmx_file}" .jmx)

    # Check if the "low" flag is provided and if the JMX file contains "96"
    if [ "$1" != "low" ] || [[ ! "$test_name" =~ "96" ]]; then
        # Run JMeter test
        jmeter -n -t "${jmx_file}" > "output_${test_name}.txt" 2>&1

        # Print a separator line in the console for better visibility
        echo "----------------------------------------"
    fi
done

echo "All tests completed."