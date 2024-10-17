/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.harry.harry2;

import java.util.ArrayList;
import java.util.List;

public class RectangleToSquares {

    // Class to store dimensions of sub-rectangles
    static class SubRectangle {
        double width;
        double height;

        SubRectangle(double width, double height) {
            this.width = width;
            this.height = height;
        }

        @Override
        public String toString() {
            return String.format("Width: %.2f, Height: %.2f", width, height);
        }
    }

    // Heron's method to compute square root
    public static double heronsSquareRoot(double single_person_area, double tolerance) {
        double x = single_person_area;
        double prevGuess;
        do {
            prevGuess = x;
            x = 0.5 * (x + single_person_area / x);  // Heron's iterative formula
        } while (Math.abs(x - prevGuess) > tolerance);
        return x;
    }

    // Function to divide the rectangle into near-square sub-rectangles
    public static List<SubRectangle> divideIntoSquares(double M, double N, int number_of_people, double tolerance) {
        List<SubRectangle> subRectangles = new ArrayList<>();

        // Compute the area of each sub-rectangle
        double totalArea = M * N;
        double areaPerSubRectangle = totalArea / number_of_people;

        // Use Heron's method to find the ideal side length for near-square sub-rectangles
        double idealSideLength = heronsSquareRoot(areaPerSubRectangle, tolerance);

        // Determine how many divisions along width and height result in near-square sub-rectangles
        int widthDivisions = (int) (M / idealSideLength);
        int heightDivisions = (int) (N / idealSideLength);



        return subRectangles;
    }

    public static void main(String[] args) {
        double M = 20;
        double N = 10;
        int numberOfSubRectangles = 6;
        double tolerance = 0.1;

        // Divide the rectangle into sub-rectangles that are near-squares
        List<SubRectangle> subRectangles = divideIntoSquares(M, N, numberOfSubRectangles, tolerance);

        // Print the dimensions of the sub-rectangles
        for (SubRectangle subRectangle : subRectangles) {
            System.out.println(subRectangle);
        }
    }
}
